#-*- encoding: utf-8 -*-
__author__ = 'Sidney'

class User():
    def __init__(self, user_id, user_name, full_name=None, website=None, bio=None,followed_by_count=None,
                 follows_count=None, media_count=None, followed_by=None, follows=None, media=None):
        self.user_id = user_id  # type: str
        self.user_name = user_name  # type: str
        self.full_name = full_name  # type: str
        self.website = website  # type: str
        self.bio = bio  # type: str
        self.followed_by_count = followed_by_count  # type: int
        self.follows_count = follows_count  # type: int
        self.media_count = media_count  # type: int
        self.followed_by = followed_by  # type: list
        self.follows = follows  # type: list
        self.media = media  # type: list


class Location():
    def __init__(self, location_id, lat, lon, name):
        self.location_id = location_id  # type: int
        self.lat = lat  # type: int
        self.lon = lon  # type: int
        self.name = name  # type: str

class Point():
    def __init__(self, x, y):
        self.x = x
        self.y = y

class Taggee():
    def __init__(self, user, position):
        self.user = user  # type: User
        self.position = position  # type: Point

class Media():
    def __init__(self, media_id, author, type, created_timestamp, urls, comments_count=None, comments=None,
                 likes_count=None, likes=None, tags=None, location=None, taggees=None):
        self.media_id = media_id  # type: int
        self.author = author  # type: User
        self.type = type  # type: str
        self.created_timestamp = created_timestamp  # type: int
        self.urls = urls  # type: dict
        self.comments_count = comments_count  # type: int
        self.comments = comments  # type: list
        self.likes_count = likes_count  # type: int
        self.likes = likes  # type: list
        self.tags = tags  # type: list
        self.location = location  # type: Location
        self.taggees = taggees  # type: list