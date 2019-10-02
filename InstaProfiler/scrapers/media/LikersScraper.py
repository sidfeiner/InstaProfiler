import logging
import uuid
from datetime import datetime
from time import sleep
from typing import Dict, Union
from selenium.webdriver.remote.webdriver import WebDriver

import json
from typing import List, Optional

from InstaProfiler.common.base import InstaUser
from InstaProfiler.scrapers.InstagramScraper import InstagramScraper, QueryHashes
from InstaProfiler.scrapers.media.base import Media

DEFAULT_BATCH_SIZE = 100
RATE_LIMIT_REACHED_WAIT_SECONDS = 1 * 60  # 3 minutes
MAX_ALLOWED_RETRIES = 10


class LikersScraping(object):
    def __init__(self, likers: List[InstaUser], scrape_id: str, scrape_ts: datetime):
        self.scrape_id = scrape_id
        self.likers = likers
        self.scrape_ts = scrape_ts


class LikersScraper(InstagramScraper):
    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True, driver: Optional[WebDriver] = None):
        super().__init__(log_path, log_level, log_to_console)
        if driver is not None:
            self.driver = driver

    @classmethod
    def _create_request_vars(cls, shortcode: str, first: int, after: Optional[str] = None):
        vars = {
            'shortcode': shortcode,
            'first': first,
            'include_reel': True
        }
        if after is not None:
            vars['after'] = after
        return vars

    @classmethod
    def create_url(cls, query_hash: str, shortcode: str, end_cursor: Optional[str] = None, batch_size: int = 15):
        vars_dict = cls._create_request_vars(shortcode, batch_size, end_cursor)
        vars_str = json.dumps(vars_dict)
        return "{url}?query_hash={hash}&variables={vars}".format(url=cls.GRAPH_URL, hash=query_hash, vars=vars_str)

    def _is_failed_response(self, body: str):
        wrong_strings = ['please wait', 'rate limited']
        normalized_body = body.lower()
        if any([x in normalized_body for x in wrong_strings]):
            return True
        return False

    def scrape_media_likers(self, media: Media, scrape_id: Optional[str] = None,
                            scrape_ts: Optional[datetime] = None,
                            batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
                            max_likers_amount: Optional[int] = None) -> LikersScraping:
        """
        :param media: Media object for which we scrape its likers
        :param batch_size: Amount of users to request in every API request (limits to 50)
        :param max_likers_amount: Once this amount has been scraped, finish. If not set, will scrape everyone
        :return:
        """
        self.init_driver()
        scrape_id = scrape_id or str(uuid.uuid4())
        scrape_ts = scrape_ts or datetime.now()
        self.logger.info('Scraping likers (scrape id %s) for media %s at %s', scrape_id, media.id, media.display_url)
        self.to_home_page()
        request_url = self.create_url(QueryHashes.MEDIA_LIKES, media.shortcode, batch_size=batch_size)
        all_users = {}  # type: Dict[int, Story]
        while True:
            self.driver.get(request_url)
            retries = 0
            body = self.driver.find_element_by_tag_name('body').text
            while self._is_failed_response(body) and retries < MAX_ALLOWED_RETRIES:
                self.logger.warn("Waiting %d seconds", RATE_LIMIT_REACHED_WAIT_SECONDS)
                sleep(RATE_LIMIT_REACHED_WAIT_SECONDS)
                retries += 1
                self.driver.get(request_url)
                body = self.driver.find_element_by_tag_name('body').text
            self.logger.info("Parsing likers from page...")

            try:
                data = json.loads(body)['data']['shortcode_media'][
                    'edge_liked_by']
            except Exception as e:
                self.logger.exception("Failed loading json. body: %s", body)
                raise e
            likers_objects = [media_object['node'] for media_object in data['edges']]
            next_cursor = data['page_info']

            for item in likers_objects:
                liker = InstaUser.from_dict(item)
                if liker.user_id not in all_users:
                    all_users[liker.user_id] = liker
            self.logger.info("Parsed %d liker objects", len(likers_objects))

            if next_cursor['has_next_page'] is False or (
                    max_likers_amount is not None and len(all_users) >= max_likers_amount):
                break
            next_cursor = next_cursor['end_cursor']
            request_url = self.create_url(QueryHashes.MEDIA_LIKES, media.shortcode, next_cursor, batch_size)

        return LikersScraping(list(all_users.values()), scrape_id, scrape_ts)
