import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Set, Dict, Union
from functools import partial
import fire
from pyodbc import Cursor
from selenium.webdriver import Chrome
import pandas as pd
from selenium.webdriver.remote.webdriver import WebDriver

from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.MySQL import MySQLHelper, InsertableDuplicate, Updatable, Insertable
from InstaProfiler.common.base import Serializable, InstaUser, UserDoesNotExist, InstaUserRecord
from InstaProfiler.scrapers.common.InstagramScraper import InstagramScraper, QueryHashes, DEFAULT_USER_NAME

LOG_PATH = "/home/sid/personal/Projects/InstaProfiler/logs/follows.log"


class UserFollows(Serializable):
    def __init__(self, user: InstaUser, followers: Set[InstaUser], follows: Set[InstaUser]):
        self.user = user
        self.followers = followers
        self.follows = follows

    def get_friends(self):
        """Users that are followed by user, and follow the user"""
        return self.followers.intersection(self.follows)

    def get_groupies(self):
        """Users that follow our user but he doesn't follow them back"""
        return self.followers.difference(self.follows)

    def get_celebs(self):
        """Users that the user follows, but they don't follow him back"""
        return self.follows.difference(self.followers)

    def __repr__(self):
        return "UserFollows[user={0}]".format(self.user)

    @classmethod
    def from_dict(cls, attr_dict: dict) -> 'UserFollows':
        user = InstaUser.from_dict(attr_dict['user'])
        followers = {InstaUser.from_dict(x) for x in attr_dict['followers']}
        follows = {InstaUser.from_dict(x) for x in attr_dict['follows']}
        return UserFollows(user, followers, follows)


class FollowScrape(object):
    def __init__(self, follows: List[UserFollows], scrape_id: str, scrape_ts: datetime):
        self.follows = follows
        self.scrape_id = scrape_id
        self.scrape_ts = scrape_ts


class UserFollowsScraper(InstagramScraper):

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        super().__init__(log_path, log_level, log_to_console)
        self.logger = LoggerManager.get_logger(__name__)

    @classmethod
    def create_url(cls, query_hash: str, user_id: str, end_cursor: Optional[str] = None, batch_size: int = 300):
        vars = {"id": user_id, "first": batch_size, "include_reel": False, "fetch_mutual": False}
        if end_cursor is not None:
            vars['after'] = end_cursor
        vars_str = json.dumps(vars)
        return "{url}?query_hash={hash}&variables={vars}".format(url=cls.GRAPH_URL, hash=query_hash, vars=vars_str)

    @staticmethod
    def is_valid_body(body: str, data_key: int) -> bool:
        data = json.loads(body)
        try:
            data['data']['user'][data_key]
            return True
        except Exception as e:
            return False

    def scrape_follow_type(self, driver: WebDriver, user_name: str, query_hash: str) -> Set[InstaUser]:
        self.init_driver()
        self.logger.info('Scraping follow type')
        driver.get("http://www.instagram.com/{0}".format(user_name))
        data_key = 'edge_followed_by' if query_hash == QueryHashes.FOLLOWERS else 'edge_follow'
        validation_func = partial(self.is_valid_body, data_key=data_key)
        user_id = self.parse_user_id_from_profile(driver)
        request_url = self.create_url(query_hash, user_id)
        all_users = set()  # type: Set[InstaUser]
        while True:
            body = self.get_url_data(request_url, validation_func)
            try:
                data = json.loads(body)['data']['user'][data_key]
                end_cursor = data['page_info']['end_cursor'] if data['page_info']['has_next_page'] else None
                users = {InstaUser.from_dict(user['node']) for user in data['edges']}
            except Exception as e:
                self.logger.exception("driver body: {0}".format(driver.find_element_by_tag_name('body').text))
                raise e
            self.logger.info('Currently scraped %d users', len(users))
            all_users.update(users)
            if end_cursor is None:
                break
            request_url = self.create_url(query_hash, user_id, end_cursor)
        self.logger.info('Done scraping users. found %d users', len(all_users))
        return all_users

    def parse_followers(self, driver: WebDriver, user_name: str) -> Set[InstaUser]:
        """Return followers of given user

        Parameters
        ----------

        driver: Chrome
        user_name: str
            User to parse. Must be user name
        """
        self.logger.info('parsing followers')
        return self.scrape_follow_type(driver, user_name, QueryHashes.FOLLOWERS)

    def parse_following(self, driver: WebDriver, user_name: str) -> Set[InstaUser]:
        """Return followers of given user

        Parameters
        ----------

        driver: Chrome
        user_name: str
            User to parse. Must be user name
        """
        self.logger.info('parsing following')
        return self.scrape_follow_type(driver, user_name, QueryHashes.FOLLOWING)

    def parse_user_follows(self, users: List[InstaUser], scrape_follows: bool = True,
                           scrape_followers: bool = True) -> FollowScrape:
        self.init_driver()
        scrape_id = str(uuid.uuid4())
        scrape_ts = datetime.now()
        users_follows = []
        for user in users:
            if scrape_follows or scrape_followers:
                self.logger.info("Scraping user follows for user %s", user)
            else:
                self.logger.info("Skipping user %s", user)
            followers = following = set()
            if scrape_follows:
                following = self.parse_following(self.driver, user.username)
            if scrape_followers:
                followers = self.parse_followers(self.driver, user.username)
            users_follows.append(UserFollows(user, followers, following))
        return FollowScrape(users_follows, scrape_id, scrape_ts)


class FollowRecord(InsertableDuplicate):
    def __init__(self, src_user_id: str, src_user_name: str, dst_user_id: str, dst_user_name: str,
                 src_follows: bool, src_follows_first_timestamp: datetime, src_follows_latest_timestamp: datetime,
                 dst_follows: bool, dst_follows_first_timestamp: datetime, dst_follows_latest_timestamp: datetime,
                 dst_unfollows_latest_timestamp: Optional[datetime] = None):
        self.src_user_id = src_user_id
        self.src_user_name = src_user_name
        self.dst_user_id = dst_user_id
        self.dst_user_name = dst_user_name
        self.src_follows = int(src_follows)
        self.src_follows_first_timestamp = src_follows_first_timestamp
        self.src_follows_latest_timestamp = src_follows_latest_timestamp
        self.dst_follows = int(dst_follows)
        self.dst_follows_first_timestamp = dst_follows_first_timestamp if dst_follows == 1 else None
        self.dst_follows_latest_timestamp = dst_follows_latest_timestamp if dst_follows == 1 else None
        self.dst_unfollows_latest_timestamp = dst_unfollows_latest_timestamp

    @classmethod
    def export_order(cls) -> List[str]:
        return ['src_user_id', 'src_user_name', 'dst_user_id', 'dst_user_name', 'src_follows',
                'src_follows_first_timestamp', 'src_follows_latest_timestamp',
                'dst_follows', 'dst_follows_first_timestamp', 'dst_follows_latest_timestamp']

    def on_duplicate_update_sql(self) -> (str, list):
        # For last part, it assumes we already updated the dst_unfollows_latest_timestamp field.
        # If it's value is the same as the scrape_ts, we mustn't update dst_follows_latest_timestamp
        # Otherwise, we do
        return "src_follows = ?, " \
               "src_follows_latest_timestamp = ifnull(?, src_follows_latest_timestamp), " \
               "dst_follows = ?, " \
               "dst_follows_latest_timestamp = ifnull(?, dst_follows_latest_timestamp), " \
               "dst_unfollows_latest_timestamp = ifnull(?, dst_unfollows_latest_timestamp)"

    def on_duplicate_update_params(self) -> List:
        return [self.src_follows, self.src_follows_latest_timestamp, self.dst_follows,
                self.dst_follows_latest_timestamp, self.dst_unfollows_latest_timestamp]


class FOLLOW_TYPE_IDS:
    FOLLOW = 1
    UNFOLLOW = 0


class RawFollowEventRecord(Insertable):
    def __init__(self, src_user_id: int, src_user_name: str, dst_user_id: int, dst_user_name: str, follow_type_id: int,
                 ts: datetime):
        self.src_user_id = src_user_id
        self.src_user_name = src_user_name
        self.dst_user_id = dst_user_id
        self.dst_user_name = dst_user_name
        self.follow_type_id = follow_type_id
        self.ts = ts

    @classmethod
    def export_order(cls) -> List[str]:
        return ["src_user_id", "src_user_name", "dst_user_id", "dst_user_name", "follow_type_id", "ts"]


class RawFollowRecord(RawFollowEventRecord):
    def __init__(self, src_user_id: int, src_user_name: str, dst_user_id: int, dst_user_name: str,
                 ts: datetime):
        super().__init__(src_user_id, src_user_name, dst_user_id, dst_user_name, FOLLOW_TYPE_IDS.FOLLOW, ts)


class RawUnfollowRecord(RawFollowEventRecord):
    def __init__(self, src_user_id: int, src_user_name: str, dst_user_id: int, dst_user_name: str,
                 ts: datetime):
        super().__init__(src_user_id, src_user_name, dst_user_id, dst_user_name, FOLLOW_TYPE_IDS.UNFOLLOW, ts)


class UpdateUnfollow(Updatable):
    def __init__(self, src_user_id: str, dst_user_id: str, dst_unfollows_latest_timestamp: datetime):
        self.src_user_id = src_user_id
        self.dst_user_id = dst_user_id
        self.dst_unfollows_latest_timestamp = dst_unfollows_latest_timestamp

    @classmethod
    def update_key(cls) -> List[str]:
        return ['src_user_id', 'dst_user_id']

    @classmethod
    def update_sql(cls) -> str:
        return "dst_unfollows_latest_timestamp = ?"

    def update_params(self) -> Optional[List]:
        return [self.dst_unfollows_latest_timestamp]


class UserFollowsAudit(object):
    FOLLOWS_TABLE = "follows"
    RAW_FOLLOWS_TABLE = "follow_events"
    USER_INFO_TABLE = "users"

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        LoggerManager.init(log_path, level=log_level, with_console=log_to_console)
        self.logger = LoggerManager.get_logger(__name__)

    def get_current_follows(self, mysql: MySQLHelper, user: str, cursor: Optional[Cursor] = None) -> Optional[
        UserFollows]:
        res = mysql.query("select * from {0} where src_user_name = ?".format(self.FOLLOWS_TABLE), [user], cursor)
        followers = set()
        follows = set()
        if len(res) == 0:
            return None
        for r in res:
            if r.dst_follows:
                followers.add(InstaUser(r.dst_user_id, r.dst_user_name))
            if r.src_follows:
                follows.add(InstaUser(r.dst_user_id, r.dst_user_name))
        return UserFollows(InstaUser(res[0].src_user_id, res[0].src_user_name, res[0].src_user_name), followers,
                           follows)

    def _update_agg_unfollowers(self, mysql: MySQLHelper, cursor: Cursor, unfollowers: Set[InstaUser], src_user: str,
                                scrape_ts: datetime, follow_side: str):
        """Update agg table with unfollowers"""
        self.logger.info("Updating unfollowers in agg table (found %d)", len(unfollowers))
        ids = ', '.join(["'{0}'".format(dst_user.user_id) for dst_user in unfollowers])
        params = [scrape_ts, src_user]
        sql = """
                                UPDATE {table}
                                SET {side}_unfollows_latest_timestamp = ?, {side}_follows = 0
                                WHERE src_user_name = ? and {side}_user_id in ({ids})
                            """.format(table=self.FOLLOWS_TABLE, ids=ids, side=follow_side)
        mysql.execute(sql, params, cursor)
        self.logger.info("Done updating agg table records")

    def update_agg_src_unfollowers(self, mysql: MySQLHelper, cursor: Cursor, unfollowers: Set[InstaUser], src_user: str,
                                   scrape_ts: datetime):
        self._update_agg_unfollowers(mysql, cursor, unfollowers, src_user, scrape_ts, "src")

    def update_agg_dst_unfollowers(self, mysql: MySQLHelper, cursor: Cursor, unfollowers: Set[InstaUser], src_user: str,
                                   scrape_ts: datetime):
        self._update_agg_unfollowers(mysql, cursor, unfollowers, src_user, scrape_ts, "dst")

    def insert_raw_unfollowers(self, mysql: MySQLHelper, cursor: Cursor, unfollowers: Set[InstaUser],
                               src_user: InstaUser, scrape_ts: datetime):
        """Insert raw records to table"""
        self.logger.info("Insert raw unfollows...")
        records = [RawUnfollowRecord(src_user.user_id, src_user.username, user.user_id, user.username, scrape_ts) for
                   user in unfollowers]
        mysql.insert(self.RAW_FOLLOWS_TABLE, records, cursor)
        self.logger.info("Done updating raw table records")

    def handle_unfollowers(self, mysql: MySQLHelper, cursor: Cursor, users: Set[InstaUser], src_user: InstaUser,
                           scrape_ts: datetime):
        self.update_agg_src_unfollowers(mysql, cursor, users, src_user.username, scrape_ts)
        self.insert_raw_unfollowers(mysql, cursor, users, src_user, scrape_ts)

    def update_agg_followers(self, mysql: MySQLHelper, cursor: Cursor, follows: List[UserFollows],
                             analyzed: (List[dict], Dict[str, InstaUser]), users: Dict[int, InstaUser],
                             unfollowers: Set[InstaUser], scrape_ts: datetime):
        self.logger.info("Inserting following records into agg table...")
        records = []
        src_user = follows[0].user
        for f in analyzed:
            dst_user = users[f['dst_id']]
            unfollow_ts = scrape_ts if dst_user in unfollowers else None
            records.append(FollowRecord(src_user.user_id, src_user.username, dst_user.user_id, dst_user.username,
                                        f['src_follows'], scrape_ts, scrape_ts, f['dst_follows'], scrape_ts, scrape_ts,
                                        unfollow_ts))

        mysql.insert_on_duplicate_update(self.FOLLOWS_TABLE, records, cursor)
        self.logger.info("done insert follows to agg table")

    def insert_raw_followers(self, mysql: MySQLHelper, cursor: Cursor, unfollowers: Set[InstaUser],
                             src_user: InstaUser, scrape_ts: datetime):
        """Insert raw records to table"""
        self.logger.info("Insert raw follows...")
        records = [RawFollowRecord(src_user.user_id, src_user.username, user.user_id, user.username, scrape_ts) for
                   user in unfollowers]
        mysql.insert(self.RAW_FOLLOWS_TABLE, records, cursor)
        self.logger.info("Done updating raw table records")

    def persist_user(self, mysql: MySQLHelper, cursor: Cursor, user: InstaUser, scrape_ts: datetime):
        self.logger.debug("persisting user to mysql...")
        user_record = InstaUserRecord.from_insta_user(scrape_ts, user)
        mysql.insert_on_duplicate_update(self.USER_INFO_TABLE, [user_record], cursor)

    def main(self, user: Union[InstaUser, str] = DEFAULT_USER_NAME, only_mutual: bool = False,
             scrape_follows: bool = True,
             scrape_followers: bool = True, max_follow_amount: Optional[int] = None,
             scraper: Optional[UserFollowsScraper] = None):
        """
        :param user: User to parse its follows.
        :param only_mutual: If set to True, will save only people that are both followers and follows.
                            Useful when src has many followers. This will make sure only "relevant" people are saved
        :param scrape_follows: If given, will only scrape user's follow
        :param scrape_followers: If given, will only scrape user's followers
        :param max_follow_amount: If given, will only scrape follows (follows/followers apart) if amount is
                                  under max_follow_amount
        :return:
        """
        scraper = scraper or UserFollowsScraper()
        if isinstance(user, str):
            user = scraper.scrape_user(user)

        if user is None:
            raise UserDoesNotExist()
        if scrape_followers and max_follow_amount is not None and user.followed_by_amount > max_follow_amount:
            self.logger.warning("user is followed by too many people (followed by %d, max %d), skipping followers...",
                                user.followed_by_amount, max_follow_amount)
            scrape_followers = False

        if scrape_follows and max_follow_amount is not None and user.follows_amount > max_follow_amount:
            self.logger.warning("user follows too many people (follows %d, max %d), skipping follows...",
                                user.follows_amount, max_follow_amount)
            scrape_follows = False

        mysql = MySQLHelper('mysql-insta-local')
        cursor = mysql.get_cursor()

        if user.is_private and not user.followed_by_viewer:
            self.logger.warning("user is private and not followed by viewer. skipping scraping...")
            scrape_ts = datetime.now()
        else:
            follow_scrape = scraper.parse_user_follows([user], scrape_follows, scrape_followers)
            follows, scrape_id, scrape_ts = follow_scrape.follows, follow_scrape.scrape_id, follow_scrape.scrape_ts

            current_follows = self.get_current_follows(mysql, user.username, cursor)
            analyzed, users = UserFollowsAnalyzer.analyze_follows(follows, only_mutual)

            # Update unfollowers
            if scrape_follows:
                # No followers are scraped so they could all be considered as unfollowed
                dst_has_unfollowed = set() if current_follows is None else current_follows.followers.difference(
                    follows[0].followers)
                if len(dst_has_unfollowed) > 0:
                    self.update_agg_dst_unfollowers(mysql, cursor, dst_has_unfollowed, user.username, scrape_ts)

            src_has_unfollowed = set() if current_follows is None else current_follows.follows.difference(
                follows[0].follows)
            if len(src_has_unfollowed):
                self.handle_unfollowers(mysql, cursor, src_has_unfollowed, user, scrape_ts)

            # Insert new records
            if sum(len(x.follows) for x in follows) > 0:
                self.update_agg_followers(mysql, cursor, follows, analyzed, users, src_has_unfollowed, scrape_ts)
                new_follows = follows[0].follows if current_follows is None else follows[0].follows.difference(
                    current_follows.follows)
                if len(new_follows) > 0:
                    self.insert_raw_followers(mysql, cursor, new_follows, user, scrape_ts)

        # Update user info
        self.persist_user(mysql, cursor, user, scrape_ts)

        mysql.commit()
        cursor.close()
        mysql.close()
        self.logger.info("Done UserFollowsScraper main")


class UserFollowsAnalyzer(object):
    @classmethod
    def analyze_follows(cls, users_follows: List[UserFollows], only_mutuals: bool = False) -> (
            List[dict], Dict[str, InstaUser]):
        all_data = []
        user_id_to_user = {}
        for user in users_follows:
            all_follows = user.follows.union(user.followers)
            for f in all_follows:
                user_id_to_user[f.user_id] = f

            user_data = []
            for f in all_follows:
                src_follows = f in user.follows
                dst_follows = f in user.followers
                if only_mutuals and False in [src_follows, dst_follows]:
                    # Do not save user if only mutuals but one doesn't follow the other
                    continue
                user_data.append(
                    {'src_id': user.user.user_id, 'src_full_name': user.user.full_name, 'dst_id': f.user_id,
                     'dst_full_name': f.full_name, 'src_follows': src_follows,
                     'dst_follows': dst_follows})
            all_data.extend(user_data)
        return all_data, user_id_to_user

    @classmethod
    def rank_mutual_follows(cls, users_follows: List[UserFollows], first_results: Optional[int] = None):
        all_data, user_id_to_user = cls.analyze_follows(users_follows)
        df = pd.DataFrame(all_data)
        only_friends = df[(df['src_follows'] == True) & (df['dst_follows'] == True)]
        analyzed = only_friends.groupby('dst_id').agg(
            {'src_id': 'count', 'src_full_name': lambda x: ','.join(x)}).sort_values(by='src_id', ascending=False)
        final_results = []
        limit_results = first_results or len(analyzed)
        for record in analyzed[:limit_results].to_records():
            user_id, count, follows = record
            final_results.append((user_id_to_user[user_id], count, follows))
        return final_results

    @classmethod
    def main(cls, username: str):
        # with open('/home/sid/Desktop/uf.json') as fp:
        #     users_follows_dict = json.load(fp)
        # users_follows = [UserFollows.from_dict(x) for x in users_follows_dict]
        users_follows = UserFollowsScraper.parse_user_follows([username])
        top_follows = cls.analyze_follows(users_follows.follows)
        for record in top_follows:
            print(record)


if __name__ == '__main__':
    fire.Fire(UserFollowsAudit)
