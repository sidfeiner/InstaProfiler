import logging
import uuid
from datetime import datetime
from typing import Dict, Union

import fire

from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.MySQL import Insertable, InsertableDuplicate, MySQLHelper
import json
from typing import List, Optional, Set

from InstaProfiler.common.base import Serializable, InstaUser
from InstaProfiler.scrapers.common.InstagramScraper import InstagramScraper, QueryHashes


class RankedUser(InstaUser):
    def __init__(self, rank: int, user_id: str, username: str, full_name: Optional[str] = None,
                 profile_pic_url: Optional[str] = None, is_private: Optional[bool] = None,
                 is_verified: Optional[bool] = None, followed_by_viewer: Optional[bool] = None, *args, **kwargs):
        super().__init__(user_id, username, full_name, profile_pic_url, is_private, is_verified, followed_by_viewer,
                         *args, **kwargs)
        self.rank = rank

    def __repr__(self):
        return "RankedUser[username={0}, rank={1}]".format(self.username, self.rank)

    @classmethod
    def from_user_and_rank(cls, user: InstaUser, rank: int):
        return RankedUser(rank, **user.__dict__)

    @classmethod
    def from_dict_and_rank(cls, attr_dict: dict, rank: int):
        user = InstaUser.from_dict(attr_dict)
        return RankedUser.from_user_and_rank(user, rank)


class Story(Serializable):
    def __init__(self, story_id: str, story_owner_id: str, story_owner_user_name: str, display_url: str,
                 taken_at_timestamp: int, expiring_at_timestamp: int,
                 story_view_count: int, is_video: bool, viewers: Optional[Set[RankedUser]] = None, *args, **kwargs):
        self.story_id = story_id
        self.story_owner_id = story_owner_id
        self.story_owner_user_name = story_owner_user_name
        self.display_url = display_url
        self.taken_at_timestamp = taken_at_timestamp
        self.expiring_at_timestamp = expiring_at_timestamp
        self.story_view_count = story_view_count
        self.is_video = is_video
        self.viewers = viewers if viewers is not None else set()  # type: Set[RankedUser]

    def update_viewers(self, viewers: Set[InstaUser]):
        self.viewers.update(viewers)

    def __hash__(self):
        return int(self.story_id)

    def __eq__(self, other: 'Story'):
        if not isinstance(other, Story):
            return False
        return self.story_id == other.story_id

    def __repr__(self):
        return "Story[id={0}, count={1}]".format(self.story_id, self.story_view_count)

    @classmethod
    def from_dict(cls, attr_dict: dict) -> 'Story':
        if 'story_id' not in attr_dict:
            attr_dict['story_id'] = attr_dict['id']
        s = super().from_dict(attr_dict)  # type: Story
        return s


class StoryScraping(object):
    def __init__(self, stories: List[Story], scrape_id: str, scrape_ts: datetime):
        self.scrape_id = scrape_id
        self.stories = stories
        self.scrape_ts = scrape_ts


class StoryViewersScraper(InstagramScraper):

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        super().__init__(log_path, log_level, log_to_console)

    @classmethod
    def _create_request_vars(cls, user_id: str, cursor: Optional[str] = None, batch_size: int = 50):
        vars = {
            'reel_ids': [user_id],
            'precomposed_overlay': False,
            'show_story_viewer_list': True,
            'story_viewer_fetch_count': batch_size
        }
        if cursor is not None:
            vars['story_viewer_cursor'] = cursor
        return vars

    @classmethod
    def create_url(cls, query_hash: str, user_id: str, end_cursor: Optional[str] = None, batch_size: int = 300):
        vars_dict = cls._create_request_vars(user_id, end_cursor, batch_size)
        vars_str = json.dumps(vars_dict)
        return "{url}?query_hash={hash}&variables={vars}".format(url=cls.GRAPH_URL, hash=query_hash, vars=vars_str)

    def scrape_viewers(self) -> StoryScraping:
        self.init_driver()
        scrape_id = str(uuid.uuid4())
        scrape_ts = datetime.now()
        self.logger.info('Scraping viewers (scrape id %s)', scrape_id)
        self.to_home_page()
        my_user_id, my_user_name = self.parse_current_user_info()
        request_url = self.create_url(QueryHashes.STORY_VIEWERS, my_user_id)
        all_stories = {}  # type: Dict[str, Story]
        while True:
            self.driver.get(request_url)
            stories_raw = json.loads(self.driver.find_element_by_tag_name('body').text)['data']['reels_media']
            if len(stories_raw) > 0:
                stories = stories_raw[0]['items']
                next_cursors = []
                for item in stories:
                    item['story_owner_id'] = my_user_id
                    item['story_owner_user_name'] = my_user_name
                    story = Story.from_dict(item)
                    if story.story_id not in all_stories:
                        all_stories[story.story_id] = story

                    story_viewers = item['edge_story_media_viewers']
                    page_info = story_viewers['page_info']
                    if page_info['has_next_page']:
                        next_cursors.append(page_info['end_cursor'])

                    viewers_edges = story_viewers['edges']
                    # Based on the fact that next_cursor is a number of the last index
                    previous_viewers_amount = len(all_stories[story.story_id].viewers)
                    viewers = {RankedUser.from_dict_and_rank(x['node'], index + previous_viewers_amount + 1) for index, x in
                               enumerate(viewers_edges)}
                    all_stories[story.story_id].update_viewers(viewers)

                if len(next_cursors) == 0:
                    # No story has more viewers
                    break
                next_cursor = next_cursors[0]  # If more than 1 exist, they will be the same
                request_url = self.create_url(QueryHashes.STORY_VIEWERS, my_user_id, next_cursor)

        return StoryScraping(list(all_stories.values()), scrape_id, scrape_ts)


class StoryRecord(Insertable):
    def __init__(self, story_id: str, story_owner_id: str, story_owner_user_name: str, display_url: str,
                 taken_at_timestamp: datetime, expiring_at_timestamp: datetime):
        self.story_id = story_id
        self.story_owner_id = story_owner_id
        self.story_owner_user_name = story_owner_user_name
        self.display_url = display_url
        self.taken_at_timestamp = taken_at_timestamp
        self.expiring_at_timestamp = expiring_at_timestamp

    @classmethod
    def export_order(cls) -> List[str]:
        return ['story_id', 'display_url', 'taken_at_timestamp', 'expiring_at_timestamp', 'story_owner_id',
                'story_owner_user_name']


class ViewerRecord(InsertableDuplicate):
    def __init__(self, scrape_id: str, scrape_ts: datetime, story_id: str, story_owner_id: str,
                 story_owner_user_name: str, view_count: int, user_id: int,
                 user_name: str, rank: int):
        self.scrape_id = scrape_id
        self.scrape_ts = scrape_ts
        self.first_seen_ts = scrape_ts
        self.story_id = story_id
        self.story_owner_id = story_owner_id
        self.story_owner_user_name = story_owner_user_name
        self.view_count = view_count
        self.user_id = user_id
        self.user_name = user_name
        self.view_rank = rank

    @classmethod
    def export_order(cls) -> List[str]:
        return ['scrape_id', 'scrape_ts', 'first_seen_ts', 'story_id', 'view_count', 'user_id', 'user_name',
                'view_rank', 'story_owner_id', 'story_owner_user_name']

    def on_duplicate_update_sql(self) -> (str, list):
        return "view_count = ?, scrape_id = ?, scrape_ts = ?, view_rank = ?"

    def on_duplicate_update_params(self) -> List:
        return [self.view_count, self.scrape_id, self.scrape_ts, self.view_rank]


class StoryViewersAudit(object):
    VIEWERS_TABLE = "story_scrapes"
    STORIES_TABLE = "stories"

    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        LoggerManager.init(log_path, level=log_level, with_console=log_to_console)
        self.logger = LoggerManager.get_logger(__name__)

    def save_results(self, scrape_result: StoryScraping, mysql_helper: MySQLHelper):
        story_records = []
        viewer_records = []
        cursor = mysql_helper.get_cursor()
        for story in scrape_result.stories:
            story_record = StoryRecord(story.story_id, story.story_owner_id, story.story_owner_user_name,
                                       story.display_url, datetime.fromtimestamp(story.taken_at_timestamp),
                                       datetime.fromtimestamp(story.expiring_at_timestamp))
            story_records.append(story_record)
            for viewer in story.viewers:
                v_record = ViewerRecord(scrape_result.scrape_id, scrape_result.scrape_ts, story.story_id,
                                        story.story_owner_id, story.story_owner_user_name, story.story_view_count,
                                        viewer.user_id, viewer.username, viewer.rank)
                viewer_records.append(v_record)

        story_cnt = mysql_helper.insert_ignore(self.STORIES_TABLE, story_records, cursor)
        viewer_cnt = mysql_helper.insert_on_duplicate_update(self.VIEWERS_TABLE, viewer_records, cursor)
        return story_cnt, viewer_cnt

    def main(self):
        mysql = MySQLHelper('mysql-insta-local')
        scraper = StoryViewersScraper()
        scrape_result = scraper.scrape_viewers()
        self.save_results(scrape_result, mysql)
        mysql.commit()
        mysql.close()


if __name__ == '__main__':
    fire.Fire(StoryViewersAudit)
