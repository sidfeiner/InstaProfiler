import json
from datetime import datetime
from decimal import Decimal
from typing import Optional


class Serializable(object):
    vertica_format = "%Y-%m-%d %H:%M:%S"

    def to_json(self, remove_nulls: bool = False, stringify_nums: bool = True) -> str:
        deep_dict = self._to_deep_dict(remove_nulls, stringify_nums)
        return json.dumps(deep_dict, ensure_ascii=False)

    @classmethod
    def dict_to_deep_dict(cls, d: dict, remove_nulls: bool = False, stringify_nums: bool = True) -> dict:
        """
        Parameters
        ----------
        d: dict
        Returns
        -------
        dict
            Nested dictionary with only number, strings and lists
        """
        new_dict = {}
        for k, v in d.items():
            if remove_nulls and v is None:
                continue
            else:
                new_dict[k] = cls.normalize_val(v, remove_nulls, stringify_nums)
        return new_dict

    @classmethod
    def normalize_val(cls, val, remove_nulls: bool = False, stringify_nums: bool = True,
                      blankify_nones: bool = True) -> str:
        if isinstance(val, Serializable):
            return val._to_deep_dict(remove_nulls, stringify_nums)
        elif isinstance(val, list) or isinstance(val, set):
            return [cls.normalize_val(item, remove_nulls, stringify_nums) for item in val]
        elif isinstance(val, str):
            return val
        elif isinstance(val, int) or isinstance(val, float) or isinstance(val, Decimal):
            if stringify_nums:
                return str(val)
            else:
                return val
        elif isinstance(val, datetime):
            return val.strftime(cls.vertica_format)
        elif val is None:
            return '' if blankify_nones else None
        elif isinstance(val, dict):
            return cls.dict_to_deep_dict(val, remove_nulls, stringify_nums)
        else:
            raise Exception("unknown type to convert to string. type: {0}, val: {1}".format(type(val), val))

    def _to_deep_dict(self, remove_nulls: bool = False, stringify_nums: bool = True) -> dict:
        """
        Returns
        -------
        dict
            Returns the objects __dict__ but where every nested object, is a __dict__ as well
        """
        d = self.__dict__
        return self.dict_to_deep_dict(d, remove_nulls, stringify_nums)

    @classmethod
    def from_json(cls, raw_json: str) -> 'Serializable':
        raw_dict = json.loads(raw_json)
        return cls.from_dict(raw_dict)

    @classmethod
    def from_dict(cls, attr_dict: dict) -> 'Serializable':
        return cls(**attr_dict)


class InstaUser(Serializable):
    def __init__(self, user_id: str, username: str, full_name: Optional[str] = None,
                 profile_pic_url: Optional[str] = None, is_private: Optional[bool] = None,
                 is_verified: Optional[bool] = None, followed_by_viewer: Optional[bool] = None, *args, **kwargs):
        self.user_id = user_id
        self.username = username
        self.full_name = full_name
        self.profile_pic_url = profile_pic_url
        self.is_private = is_private
        self.is_verified = is_verified
        self.followed_by_viewer = followed_by_viewer

    def __eq__(self, other: 'InstaUser'):
        if not isinstance(other, InstaUser):
            return False
        return self.user_id == other.user_id

    def __hash__(self):
        return int(self.user_id)

    def __repr__(self):
        return "InstaUser[username={0}]".format(self.username)

    @classmethod
    def from_dict(cls, attr_dict: dict) -> 'InstaUser':
        if 'user_id' not in attr_dict:
            attr_dict['user_id'] = attr_dict['id']
        return super().from_dict(attr_dict)
