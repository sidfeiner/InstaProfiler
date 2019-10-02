from InstaProfiler.common.base import InstaUser, Serializable
from datetime import datetime
from typing import List, Optional


class Comment(Serializable):
    def __init__(self, id: int, owner: InstaUser, ts: datetime, txt: Optional[str] = None):
        self.id = id
        self.owner = owner
        self.ts = ts
        self.txt = txt


class MediaTypes:
    VIDEO = "video"
    PICTURE = "picture"
    ALBUM = "album"


class Media(Serializable):
    def __init__(self, id: int, display_url: str, comments_amount: int,
                 taken_at_timestamp: datetime, likes_amount: int, owner: InstaUser, shortcode: str,
                 taggees: List[InstaUser] = None, comments: List[Comment] = None, likers: List[InstaUser] = None,
                 *args, **kwargs
                 ):
        self.id = int(id)
        if kwargs.get('is_video'):
            self.media_type = MediaTypes.VIDEO
        elif 'edge_sidecar_to_children' in kwargs:
            self.media_type = MediaTypes.ALBUM
        else:
            self.media_type = MediaTypes.PICTURE
        self.display_url = display_url
        self.comments_amount = comments_amount
        self.taken_at_timestamp = taken_at_timestamp
        self.likes_amount = likes_amount
        self.owner = owner
        self.shortcode = shortcode
        self.taggees = taggees or list()
        self.comments = comments or list()
        self.likers = likers or list()

    @classmethod
    def from_dict(cls, attr_dict: dict) -> 'Media':
        attr_dict['id'] = int(attr_dict['id'])
        attr_dict['owner'] = InstaUser.from_dict(attr_dict['owner'])
        attr_dict['comments_amount'] = attr_dict['edge_media_to_comment']['count']
        attr_dict['likes_amount'] = attr_dict['edge_media_preview_like']['count']
        attr_dict['taken_at_timestamp'] = datetime.fromtimestamp(attr_dict['taken_at_timestamp'])
        attr_dict['taggees'] = [InstaUser.from_dict(obj['node']['user']) for obj in
                                attr_dict['edge_media_to_tagged_user']['edges']]
        return super().from_dict(attr_dict)
