import json
from typing import List, Optional, Set
from selenium.webdriver import Chrome
import pandas as pd
from selenium.webdriver.remote.webdriver import WebDriver

from InstaProfiler.common.base import Serializable, InstaUser
from InstaProfiler.scrapers.InstagramScraper import InstagramScraper, QueryHashes


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


class UserFollowsScraper(InstagramScraper):



    @classmethod
    def create_url(cls, query_hash: str, user_id: str, end_cursor: Optional[str] = None, batch_size: int = 300):
        vars = {"id": user_id, "first": batch_size, "include_reel": False, "fetch_mutual": False}
        if end_cursor is not None:
            vars['after'] = end_cursor
        vars_str = json.dumps(vars)
        return "{url}?query_hash={hash}&variables={vars}".format(url=cls.GRAPH_URL, hash=query_hash, vars=vars_str)

    @classmethod
    def scrape_follow_type(cls, driver: WebDriver, user_name: str, query_hash: str) -> Set[InstaUser]:
        cls.init_driver()
        cls.logger.info('Scraping follow type')
        driver.get("http://www.instagram.com/{0}".format(user_name))
        data_key = 'edge_followed_by' if query_hash == QueryHashes.FOLLOWERS else 'edge_follow'
        user_id = cls.parse_user_id_from_profile(driver)
        request_url = cls.create_url(query_hash, user_id)
        all_users = set()  # type: Set[InstaUser]
        while True:
            driver.get(request_url)
            data = json.loads(driver.find_element_by_tag_name('body').text)['data']['user'][data_key]
            end_cursor = data['page_info']['end_cursor'] if data['page_info']['has_next_page'] else None
            users = {InstaUser.from_dict(user['node']) for user in data['edges']}
            cls.logger.info('Currently scraped %d users', len(users))
            all_users.update(users)
            if end_cursor is None:
                break
            request_url = cls.create_url(query_hash, user_id, end_cursor)
        cls.logger.info('Done scraping users. found %d users', len(all_users))
        return all_users

    @classmethod
    def parse_followers(cls, driver: WebDriver, user_name: str) -> Set[InstaUser]:
        """Return followers of given user

        Parameters
        ----------

        driver: Chrome
        user_name: str
            User to parse. Must be user name
        """
        cls.logger.info('parsing followers')
        return cls.scrape_follow_type(driver, user_name, QueryHashes.FOLLOWERS)

    @classmethod
    def parse_following(cls, driver: WebDriver, user_name: str) -> Set[InstaUser]:
        """Return followers of given user

        Parameters
        ----------

        driver: Chrome
        user_name: str
            User to parse. Must be user name
        """
        cls.logger.info('parsing following')
        return cls.scrape_follow_type(driver, user_name, QueryHashes.FOLLOWING)



    @classmethod
    def parse_user_follows(cls, *user_names: str):
        cls.init_driver()
        users_follows = []
        for user_name in user_names:
            user = cls.scrape_user(cls.driver, user_name)
            followers = cls.parse_followers(cls.driver, user_name)
            following = cls.parse_following(cls.driver, user_name)
            users_follows.append(UserFollows(user, followers, following))
        return users_follows


def analyze_follows(users_follows: List[UserFollows], first_results: int = 100):
    all_data = []
    user_id_to_user = {}
    for user in users_follows:
        all_follows = user.follows.union(user.followers)
        for f in all_follows:
            user_id_to_user[f.user_id] = f
        user_data = [{'src_id': user.user.user_id, 'src_full_name': user.user.full_name, 'dst_id': f.user_id,
                      'dst_full_name': f.full_name, 'src_follows': f in user.follows,
                      'dst_follows': f in user.followers} for f in all_follows]
        all_data.extend(user_data)
    df = pd.DataFrame(all_data)
    only_friends = df[(df['src_follows'] == True) & (df['dst_follows'] == True)]
    analyzed = only_friends.groupby('dst_id').agg(
        {'src_id': 'count', 'src_full_name': lambda x: ','.join(x)}).sort_values(by='src_id', ascending=False)
    final_results = []
    for record in analyzed[:first_results].to_records():
        user_id, count, follows = record
        final_results.append((user_id_to_user[user_id], count, follows))
    return final_results


def main():
    # with open('/home/sid/Desktop/uf.json') as fp:
    #     users_follows_dict = json.load(fp)
    # users_follows = [UserFollows.from_dict(x) for x in users_follows_dict]
    users_follows = UserFollowsScraper.parse_user_follows('ori_barkan1', 'rayabarkan', 'roy_barkan')
    top_follows = analyze_follows(users_follows, 50)
    for record in top_follows:
        print(record)


if __name__ == '__main__':
    main()
