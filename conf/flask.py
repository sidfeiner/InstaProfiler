#-*-encoding: utf-8 -*-
from flask import Flask
from flask_mongoengine import MongoEngine
from conf import constants
from datetime import datetime

__author__ = "Sidney"

app = Flask(__name__)
app.config['MONGODB_SETTINGS'] = {
    'db': 'app',
    'host': constants.mongo_host,
    'port': 27017,
    'username': constants.mongo_user,
    'password': constants.mongo_pwd
}
db = MongoEngine()
db.init_app(app)

class Location(db.Document):
    location_id = db.IntField(required=True)  # type: int
    lat = db.FloatField()  # type: float
    lon = db.FloatField()  # type: float
    name = db.StringField(required=False)  # type: str


class Point(db.Document):
    x = db.FloatField()
    y = db.FloatField()


class User(db.Document):
    user_id = db.StringField(required=True)
    user_name = db.StringField(required=True)
    full_name = db.StringField(required=False)  # type: str
    website = db.StringField(required=False)  # type: str
    bio = db.StringField(required=False)  # type: str
    followed_by_count = db.IntField(required=False)  # type: int
    follows_count = db.IntField(required=False)  # type: int
    media_count = db.IntField(required=False)  # type: int
    followed_by = db.ListField(db.StringField(), required=False)  # List of usernames
    follows = db.ListField(db.StringField(), required=False)  # type: list
    media = db.ListField(db.StringField(), required=False)  # type: list


class Taggee(db.Document):
    user = db.ReferenceField(User)  # type: User
    position = db.ReferenceField(Point)  # type: Point


class Comment(db.Document):
    text = db.StringField()
    author = db.ReferenceField(User)
    timestamp = db.IntField()


class Like(db.Document):
    author = db.ReferenceField(User)
    timestamp = db.IntField()


class Media(db.Document):
    media_id = db.IntField(required=True)  # type: int
    author = db.ReferenceField(User, required=True)  # type: User
    type = db.StringField(required=True)  # type: str
    created_timestamp = db.IntField(required=True)  # type: int
    #urls = urls  # type: dict
    comments_count = db.IntField()  # type: int
    comments = db.ListField(db.ReferenceField(Comment))  # type: list
    likes_count = db.IntField()  # type: int
    likes = db.ListField(db.ReferenceField(Like))  # type: list
    tags = db.ListField(db.StringField())  # type: list
    location = db.ReferenceField(Location)  # type: Location
    taggees = db.ListField(db.ReferenceField(Taggee))  # type: list


class Token(db.Document):
    username = db.StringField(required=True)
    user_id = db.IntField(required=True)
    code = db.StringField(required=True)
    access_token = db.StringField(required=True)
    timestamp = db.DateTimeField(default=datetime.now)