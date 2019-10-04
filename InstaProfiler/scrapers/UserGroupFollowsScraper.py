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
        order by last_scrape asc;
    """

    def get_users(self, group_name: str, mysql: MySQLHelper) -> List[InstaUser]:
        """Gets users to scrape it's followers. Ordered by ascending last_scrape_ts
        So it will start parsing users we haven't scraped lately
        """
        self.logger.debug("Getting users for group %s", group_name)
        res = mysql.query(self.GET_USERS_QUERY, [group_name])
        users = [InstaUser(row.user_id, row.user_name) for row in res]
        self.logger.debug("Done querying users")
        return users

    def main(self, group_name: str, scrape_follows: bool = True, scrape_followers: bool = True):
        mysql = MySQLHelper('mysql-insta-local')
        users = self.get_users(group_name, mysql)
        self.logger.info("Found %d users for group %s", len(users), group_name)
        core_scraper = UserFollowsScraper()
        scraper = UserFollowsAudit()
        for user in users:
            self.logger.info("Handling user %s", user.username)
            try:
                scraper.main(user.username, scrape_follows=scrape_follows, scrape_followers=scrape_followers,
                             scraper=core_scraper)
            except UserDoesNotExist as e:
                self.logger.warning("User %s does not exist. skip.", user.username)
            self.logger.info("Done handling user %s", user.username)


if __name__ == '__main__':
    fire.Fire(UserGroupFollowsScraper)
