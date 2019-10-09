from typing import Optional, Union, List
import logging
import fire

from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.MySQL import MySQLHelper
from InstaProfiler.common.base import InstaUser, UserDoesNotExist
from InstaProfiler.scrapers.common.UserFollowsScraper import UserFollowsScraper, UserFollowsAudit
from InstaProfiler.scrapers.common.media.MediaScraper import MediaScraper, MediaLikersAudit


class UserGroupMediaScraper(object):
    """
    Parse all follows of users who are in a group
    """

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        LoggerManager.init(log_path, level=log_level, with_console=log_to_console)
        self.logger = LoggerManager.get_logger(__name__)

    GET_USERS_QUERY = """
        select ug.user_id, ug.user_name, max(scrape_ts) as last_scrape
        from user_groups ug
                 left join media f on ug.user_id = f.owner_user_id
        where group_name = ?
        group by ug.user_id, ug.user_name
        order by last_scrape asc
    """

    def get_users(self, group_name: str, mysql: MySQLHelper,
                  limit: Optional[int] = None) -> List[InstaUser]:
        """Gets users to scrape it's media objects. Ordered by ascending last_scrape_ts
        So it will start parsing users we haven't scraped lately
        """
        self.logger.debug("Getting users for group %s", group_name)
        query = self.GET_USERS_QUERY
        if limit is not None:
            query += " limit {}".format(limit)
        params = [group_name]
        res = mysql.query(query, params)
        users = [InstaUser(row.user_id, row.user_name) for row in res]
        self.logger.debug("Done querying users")
        return users

    def main(self, group_name: str, limit_users: Optional[int] = None, max_media_to_scrape_amount: Optional[int] = 500,
             scrape_likers: bool = False, scrape_comments: bool = False, likers_threshold: Optional[int] = None,
             media_likers_limit: Optional[int] = None
             ):
        mysql = MySQLHelper('mysql-insta-local')
        users = self.get_users(group_name, mysql, limit=limit_users)
        self.logger.info("Found %d users for group %s", len(users), group_name)
        core_scraper = MediaScraper()
        scraper = MediaLikersAudit()
        for user in users:
            self.logger.info("Handling user %s", user.username)
            try:
                scraper.main(user, core_scraper, scrape_likers, scrape_comments, likers_threshold, media_likers_limit,
                             max_media_to_scrape_amount)
            except UserDoesNotExist as e:
                self.logger.warning("User %s does not exist. skip.", user.username)
            self.logger.info("Done handling user %s", user.username)
        self.logger.info("Done scraping group media")


if __name__ == '__main__':
    fire.Fire(UserGroupMediaScraper)
