import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Set, Dict, Union

import fire
from pyodbc import Cursor
from selenium.webdriver import Chrome
import pandas as pd
from selenium.webdriver.remote.webdriver import WebDriver

from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.MySQL import MySQLHelper, InsertableDuplicate, Updatable
from InstaProfiler.common.base import Serializable, InstaUser
from InstaProfiler.scrapers.InstagramScraper import InstagramScraper, QueryHashes, DEFAULT_USER_NAME

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

    def scrape_follow_type(self, driver: WebDriver, user_name: str, query_hash: str) -> Set[InstaUser]:
        self.init_driver()
        self.logger.info('Scraping follow type')
        driver.get("http://www.instagram.com/{0}".format(user_name))
        data_key = 'edge_followed_by' if query_hash == QueryHashes.FOLLOWERS else 'edge_follow'
        user_id = self.parse_user_id_from_profile(driver)
        request_url = self.create_url(query_hash, user_id)
        all_users = set()  # type: Set[InstaUser]
        while True:
            driver.get(request_url)
            try:
                data = json.loads(driver.find_element_by_tag_name('body').text)['data']['user'][data_key]
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

    def parse_user_follows(self, user_names: List[str], only_follows: bool = False,
                           only_followers: bool = False) -> FollowScrape:
        self.init_driver()
        scrape_id = str(uuid.uuid4())
        scrape_ts = datetime.now()
        users_follows = []
        for user_name in user_names:
            self.logger.info("Scraping user follows for user %s", user_name)
            user = self.scrape_user(self.driver, user_name)
            followers = following = set()
            if not only_follows:
                followers = self.parse_followers(self.driver, user_name)
            if not only_followers:
                following = self.parse_following(self.driver, user_name)
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

    def main(self, user: str = DEFAULT_USER_NAME, only_mutual: bool = False, only_follows: bool = False,
             only_followers: bool = False):
        """
        :param user: User to parse its follows
        :param only_mutual: If set to True, will save only people that are both followers and follows.
                            Useful when src has many followers. This will make sure only "relevant" people are saved
        :return:
        """
        scraper = UserFollowsScraper()
        follow_scrape = scraper.parse_user_follows([user], only_follows, only_followers)
        follows, scrape_id, scrape_ts = follow_scrape.follows, follow_scrape.scrape_id, follow_scrape.scrape_ts

        mysql = MySQLHelper('mysql-insta-local')
        cursor = mysql.get_cursor()
        current_follows = self.get_current_follows(mysql, user)
        analyzed, users = UserFollowsAnalyzer.analyze_follows(follows, only_mutual)

        # Update unfollowers
        dst_who_unfollowed = set() if current_follows is None else current_follows.followers.difference(
            follows[0].followers)
        if len(dst_who_unfollowed) > 0:
            self.logger.info("Updating unfollowers (found %d)", len(dst_who_unfollowed))
            ids = ', '.join(["'{0}'".format(dst_user.user_id) for dst_user in dst_who_unfollowed])
            params = [scrape_ts, user]
            sql = """
                        UPDATE {table}
                        SET dst_unfollows_latest_timestamp = ?, dst_follows = 0
                        WHERE src_user_name = ? and dst_user_id in ({ids})
                    """.format(table=self.FOLLOWS_TABLE, ids=ids)
            mysql.execute(sql, params, cursor)
            self.logger.info("Done updating records")

        # Insert new records
        self.logger.info("Inserting records...")
        records = []
        src_user = follows[0].user
        for f in analyzed:
            dst_user = users[f['dst_id']]
            unfollow_ts = scrape_ts if dst_user in dst_who_unfollowed else None
            records.append(FollowRecord(src_user.user_id, src_user.username, dst_user.user_id, dst_user.username,
                                        f['src_follows'], scrape_ts, scrape_ts, f['dst_follows'], scrape_ts, scrape_ts,
                                        unfollow_ts))

        mysql.insert_on_duplicate_update(self.FOLLOWS_TABLE, records, cursor)
        self.logger.info("Done inserting records.")

        mysql.commit()
        cursor.close()
        mysql.close()
        self.logger.info("DONE")


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
    def main(cls, user: str):
        # with open('/home/sid/Desktop/uf.json') as fp:
        #     users_follows_dict = json.load(fp)
        # users_follows = [UserFollows.from_dict(x) for x in users_follows_dict]
        users_follows = UserFollowsScraper.parse_user_follows([user])
        top_follows = cls.analyze_follows(users_follows.follows)
        for record in top_follows:
            print(record)


if __name__ == '__main__':
    fire.Fire(UserFollowsAudit)
