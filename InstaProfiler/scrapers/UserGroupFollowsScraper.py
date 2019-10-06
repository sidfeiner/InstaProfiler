from typing import Optional, Union, List
import logging
import fire

from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.MySQL import MySQLHelper
from InstaProfiler.common.base import InstaUser, UserDoesNotExist
from InstaProfiler.scrapers.common.UserFollowsScraper import UserFollowsScraper, UserFollowsAudit


class UserGroupFollowsScraper(object):
    """
    Parse all follows of users who are in a group
    """

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        LoggerManager.init(log_path, level=log_level, with_console=log_to_console)
        self.logger = LoggerManager.get_logger(__name__)

    GET_USERS_QUERY = """
        select ug.user_id, ug.user_name, max(src_follows_latest_timestamp) as last_scrape
        from user_groups ug
                 left join follows f on ug.user_id = f.src_user_id
        where group_name = ?
        group by ug.user_id, ug.user_name
        order by last_scrape asc
    """

    GET_USERS_WITH_LIMITED_FOLLOWS_QUERY = """
    select * from (
    {0}
    ) a
    where not exists(select 1 from users u where u.user_id = a.user_id and u.follows_amount > ?)
    order by last_scrape asc
    """.format(GET_USERS_QUERY)

    def get_users(self, group_name: str, mysql: MySQLHelper, max_follows: Optional[int] = None,
                  limit: Optional[int] = None) -> List[InstaUser]:
        """Gets users to scrape it's followers. Ordered by ascending last_scrape_ts
        So it will start parsing users we haven't scraped lately
        """
        self.logger.debug("Getting users for group %s", group_name)
        query = self.GET_USERS_WITH_LIMITED_FOLLOWS_QUERY if max_follows is not None else self.GET_USERS_QUERY
        if limit is not None:
            query += " limit {}".format(limit)
        params = [group_name]
        if max_follows:
            params.append(max_follows)
        res = mysql.query(query, params)
        users = [InstaUser(row.user_id, row.user_name) for row in res]
        self.logger.debug("Done querying users")
        return users

    def main(self, group_name: str, scrape_follows: bool = True, scrape_followers: bool = True,
             limit_users: Optional[int] = None, max_follows_to_scrape_amount: Optional[int] = 500):
        mysql = MySQLHelper('mysql-insta-local')
        users = self.get_users(group_name, mysql, max_follows=max_follows_to_scrape_amount, limit=limit_users)
        self.logger.info("Found %d users for group %s", len(users), group_name)
        core_scraper = UserFollowsScraper()
        scraper = UserFollowsAudit()
        for user in users:
            self.logger.info("Handling user %s", user.username)
            try:
                scraper.main(user.username, scrape_follows=scrape_follows, scrape_followers=scrape_followers,
                             scraper=core_scraper, max_follow_amount=max_follows_to_scrape_amount)
            except UserDoesNotExist as e:
                self.logger.warning("User %s does not exist. skip.", user.username)
            self.logger.info("Done handling user %s", user.username)


if __name__ == '__main__':
    fire.Fire(UserGroupFollowsScraper)
