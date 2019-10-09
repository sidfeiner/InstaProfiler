import logging
import uuid
from datetime import datetime
from typing import Dict, Union

import fire
from pyodbc import Row

from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.MySQL import InsertableDuplicate, MySQLHelper
import json
from typing import List, Optional

from InstaProfiler.common.base import InstaUser, Serializable
from InstaProfiler.scrapers.common.InstagramScraper import InstagramScraper, QueryHashes
from InstaProfiler.scrapers.common.media.LikersScraper import LikersScraper
from InstaProfiler.scrapers.common.media.base import Media


class MediaLikersScraping(object):
    def __init__(self, media_likes: List[Media], scrape_id: str, scrape_ts: datetime):
        self.scrape_id = scrape_id
        self.media = media_likes
        self.scrape_ts = scrape_ts


class MediaScraper(InstagramScraper):

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        self._log_path = log_path
        self._log_level = log_level
        self._log_to_console = log_to_console
        super().__init__(log_path, log_level, log_to_console)

    @classmethod
    def _create_request_vars(cls, user_id: str, first: int, after: Optional[str] = None):
        vars = {
            'id': user_id,
            'first': first,
            'after': after
        }
        return vars

    @classmethod
    def create_url(cls, query_hash: str, user_id: int, after_cursor: Optional[str] = None, batch_size: int = 15):
        vars_dict = cls._create_request_vars(user_id, batch_size, after_cursor)
        vars_str = json.dumps(vars_dict)
        return "{url}?query_hash={hash}&variables={vars}".format(url=cls.GRAPH_URL, hash=query_hash, vars=vars_str)

    def scrape_media(self, user: InstaUser, scrape_likers: bool = True, scrape_comments: bool = True,
                     likers_scrape_threshold: Optional[int] = None, media_max_likers_amount: Optional[int] = None,
                     max_media_limit: Optional[int] = None
                     ) -> MediaLikersScraping:
        """
        :param user: User whose media we want to parse
        :param scrape_likers: If True, will scrape media's likers
        :param scrape_comments: If True, will scrape media's comments
        :param likers_scrape_threshold: If media has more than this threshold's likers, likers will NOT be scraped,
                                        disregarding the `scrape_likers` param
        :param media_max_likers_amount: If media's likers will be parsed and this parameter is set, this will
                                            be the limit of likers that can be parsed
        :param max_media_limit: If set, this will be the maximum amount of media objects to parse
        """
        self.init_driver()
        scrape_id = str(uuid.uuid4())
        scrape_ts = datetime.now()
        self.logger.info('Scraping viewers (scrape id %s)', scrape_id)
        self.to_home_page()
        # my_user_id, my_user_name = self.parse_current_user_info()
        request_url = self.create_url(QueryHashes.MEDIA, user.user_id)
        all_media = {}  # type: Dict[int, Story]

        if scrape_likers:
            self.logger.info("Initing LikersScaper...")
            likers_scraper = LikersScraper(self._log_path, self._log_level, self._log_to_console, driver=self.driver)
        while True:
            self.logger.info("Parsing media from page...")
            data = self.get_url_json_data(request_url, lambda x: 'data' in x)['data']['user']['edge_owner_to_timeline_media']
            media_objects = [media_object['node'] for media_object in data['edges']]
            next_cursor = data['page_info']

            for item in media_objects:
                media = Media.from_dict(item)
                if media.id not in all_media:
                    all_media[media.id] = media
                if scrape_likers and (likers_scrape_threshold is None or media.likes_amount <= likers_scrape_threshold):
                    media.likers = likers_scraper.scrape_media_likers(media, scrape_id, scrape_ts,
                                                                      max_likers_amount=media_max_likers_amount).likers
                    self.logger.info("Found %d likers for media %d at url %s", len(media.likers), media.id,
                                     media.display_url)
            self.logger.info("Parsed %d media objects", len(media_objects))

            if next_cursor['has_next_page'] is False or (
                    max_media_limit is not None and len(all_media) >= max_media_limit):
                break
            next_cursor = next_cursor['end_cursor']
            request_url = self.create_url(QueryHashes.MEDIA, user.user_id, next_cursor)

        return MediaLikersScraping(list(all_media.values()), scrape_id, scrape_ts)


class MediaRecord(InsertableDuplicate, Serializable):
    def __init__(self, scrape_id: str, scrape_ts: datetime, media_id: int, media_type: str, taken_at_ts: datetime,
                 owner_user_name: str, owner_user_id: int, display_url: str, comments_amount: int,
                 likes_amount: int, taggees_amount: int):
        self.scrape_id = scrape_id
        self.scrape_ts = scrape_ts
        self.media_id = media_id
        self.media_type = media_type
        self.taken_at_ts = taken_at_ts
        self.owner_user_id = owner_user_id
        self.owner_user_name = owner_user_name
        self.display_url = display_url
        self.comments_amount = comments_amount
        self.likes_amount = likes_amount
        self.taggees_amount = taggees_amount

    @classmethod
    def export_order(cls) -> List[str]:
        return ['scrape_id', 'scrape_ts', 'media_id', 'media_type', 'taken_at_ts', 'owner_user_id', 'owner_user_name',
                'display_url', 'comments_amount', 'likes_amount', 'taggees_amount']

    def on_duplicate_update_sql(self) -> (str, list):
        return "comments_amount = ?, likes_amount = ?, taggees_amount = ?, scrape_id = ?, scrape_ts = ?"

    def on_duplicate_update_params(self) -> List:
        return [self.comments_amount, self.likes_amount, self.taggees_amount, self.scrape_id, self.scrape_ts]

    @classmethod
    def from_row(cls, row: Row) -> 'MediaRecord':
        row_as_dict = {col[0]: getattr(row, col[0]) for col in row.cursor_description}
        return cls.from_dict(row_as_dict)  # type: Media


class TaggeeRecord(InsertableDuplicate):
    def __init__(self, scrape_id: str, scrape_ts: datetime, media_id: int, media_type: str, media_owner_user_id: int,
                 media_owner_user_name: str, media_ts: datetime, taggee_user_id: int, taggee_user_name: str):
        self.scrape_id = scrape_id
        self.scrape_ts = scrape_ts
        self.media_id = media_id
        self.media_type = media_type
        self.media_owner_user_id = media_owner_user_id
        self.media_owner_user_name = media_owner_user_name
        self.media_ts = media_ts
        self.taggee_user_id = taggee_user_id
        self.taggee_user_name = taggee_user_name

    @classmethod
    def export_order(cls) -> List[str]:
        return ['scrape_id', 'scrape_ts', 'media_id', 'media_type', 'media_owner_user_id', 'media_owner_user_name',
                'media_ts', 'taggee_user_id', 'taggee_user_name']

    def on_duplicate_update_sql(self) -> (str, list):
        return "scrape_id = ?, scrape_ts = ?"

    def on_duplicate_update_params(self) -> List:
        return [self.scrape_id, self.scrape_ts]



class MediaOperations:
    LIKE = "like"
    COMMENT = "comment"


class MediaOperationRecord(InsertableDuplicate):
    def __init__(self, scrape_id: str, scrape_ts: datetime, media_id: int, operation: str,
                 media_owner_user_id: int, media_owner_user_name: str, interactor_id: int, interactor_user_name: str):
        self.scrape_id = scrape_id
        self.scrape_ts = scrape_ts
        self.media_id = media_id
        self.first_seen_ts = scrape_ts
        self.operation = operation
        self.media_owner_user_id = media_owner_user_id
        self.media_owner_user_name = media_owner_user_name
        self.interactor_id = interactor_id
        self.interactor_user_name = interactor_user_name

    @classmethod
    def export_order(cls) -> List[str]:
        return ['scrape_id', 'scrape_ts', 'media_id', 'first_seen_ts', 'operation', 'media_owner_user_id',
                'media_owner_user_name', 'interactor_id', 'interactor_user_name']

    def on_duplicate_update_sql(self) -> (str, list):
        return "scrape_id = ?, scrape_ts = ?"

    def on_duplicate_update_params(self) -> List:
        return [self.scrape_id, self.scrape_ts]


class MediaLikeRecord(MediaOperationRecord):
    def __init__(self, scrape_id: str, scrape_ts: datetime, media_id: int, media_owner_user_id: int,
                 media_owner_user_name: str, interactor_id: int, interactor_user_name: str):
        super().__init__(scrape_id, scrape_ts, media_id, MediaOperations.LIKE, media_owner_user_id,
                         media_owner_user_name, interactor_id, interactor_user_name)


class MediaCommentRecord(MediaOperationRecord):
    def __init__(self, scrape_id: str, scrape_ts: datetime, media_id: int, media_owner_user_id: int,
                 media_owner_user_name: str, interactor_id: int, interactor_user_name: str):
        super().__init__(scrape_id, scrape_ts, media_id, MediaOperations.COMMENT, media_owner_user_id,
                         media_owner_user_name, interactor_id, interactor_user_name)


class MediaLikersAudit(object):
    MEDIA_TABLE = "media"
    MEDIA_INTERACTIONS_TABLE = "media_interaction"
    TAGGEES_TABLE = "taggees"

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        LoggerManager.init(log_path, level=log_level, with_console=log_to_console)
        self.logger = LoggerManager.get_logger(__name__)

    def save_results(self, scrape_result: MediaLikersScraping, mysql_helper: MySQLHelper):
        """Persist media records, taggees and media interactions. Returns tuple with counts of inserted records"""
        media_records = []
        media_interactions_records = []
        taggees_records = []
        cursor = mysql_helper.get_cursor()
        for media in scrape_result.media:
            media_records.append(
                MediaRecord(scrape_result.scrape_id, scrape_result.scrape_ts, media.id, media.media_type,
                            media.taken_at_timestamp, media.owner.username, media.owner.user_id,
                            media.display_url, media.comments_amount, media.likes_amount, len(media.taggees)))
            taggees_records.extend([TaggeeRecord(scrape_result.scrape_id, scrape_result.scrape_ts, media.id,
                                                 media.media_type, media.owner.user_id, media.owner.username,
                                                 media.taken_at_timestamp, taggee.user_id, taggee.username) for taggee
                                    in media.taggees])
            for liker in media.likers:
                media_interactions_records.append(MediaLikeRecord(
                    scrape_result.scrape_id, scrape_result.scrape_ts, media.id, media.owner.user_id,
                    media.owner.username, liker.user_id, liker.username
                ))
            for commenter in media.comments:
                media_interactions_records.append(MediaCommentRecord(
                    scrape_result.scrape_id, scrape_result.scrape_ts, media.id, media.owner.user_id,
                    media.owner.username, commenter.user_id, commenter.username
                ))
        media_cnt = taggees_cnt = operations_cnt = 0
        if len(media_records) > 0:
            media_cnt = mysql_helper.insert_on_duplicate_update(self.MEDIA_TABLE, media_records, cursor)
        if len(taggees_records) > 0:
            taggees_cnt = mysql_helper.insert_on_duplicate_update(self.TAGGEES_TABLE, taggees_records, cursor)
        if len(media_interactions_records) > 0:
            operations_cnt = mysql_helper.insert_on_duplicate_update(self.MEDIA_INTERACTIONS_TABLE,
                                                                     media_interactions_records, cursor)
        return media_cnt, taggees_cnt, operations_cnt

    def main(self, user: InstaUser, scraper: Optional[MediaScraper] = None, scrape_likers: bool = False,
             scrape_comments: bool = False, likers_scrape_threshold: Optional[int] = None,
             media_max_likers_amount: Optional[int] = None, max_media_limit: Optional[int] = None):
        scraper = scraper or MediaScraper()

        if not user.from_full_profile:
            user = scraper.scrape_user(user.username)

        if user.is_private and not user.followed_by_viewer:
            self.logger.warning("user is private and not followed by viewer. skipping scraping...")
        else:
            mysql = MySQLHelper('mysql-insta-local')
            scrape_result = scraper.scrape_media(user, scrape_likers, scrape_comments, likers_scrape_threshold,
                                                 media_max_likers_amount, max_media_limit)
            self.save_results(scrape_result, mysql)
            mysql.commit()
            mysql.close()


if __name__ == '__main__':
    fire.Fire(MediaLikersAudit)
