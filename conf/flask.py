#-*-encoding: utf-8 -*-
from flask import Flask
from flask_mongoengine import MongoEngine
from conf import constants
from json import dumps
from datetime import datetime

__author__ = "Sidney"

app = Flask("InstaProfiler")
app.config['MONGODB_SETTINGS'] = {
    'db': 'dominos',
    'host': constants.mongo_host,
    'port': constants.mongo_port,
    'username': constants.mongo_user,
    'password': constants.mongo_pwd
}
db = MongoEngine()
db.init_app(app)


class DeserializableDocument(db.Document):
    @classmethod
    def from_dict(cls, data_dict):
        """
        turn dict to json string, and use built-in from_json method
        """
        json_str = dumps(data_dict)
        return cls.from_json(json_str)


class Location(db.Document):
    location_id = db.IntField(required=True)  # type: int
    latitude = db.FloatField()  # type: float
    longitude = db.FloatField()  # type: float
    name = db.StringField(required=False)  # type: str

    @classmethod
    def from_dict(cls, data_dict: dict):
        """
        :param loc_dict: dict with following keys: latitude, longitude, name, id
        :return: Location instance
        """
        data_dict['location_id'] = data_dict['id']
        return super(Location, cls).from_dict(data_dict)


class Point(db.EmbeddedDocument):
    x = db.FloatField()
    y = db.FloatField()


class User(db.Document):
    id = db.StringField(required=True, primary_key=True)
    username = db.StringField(required=True)
    full_name = db.StringField(required=False)  # type: str
    website = db.StringField(required=False)  # type: str
    bio = db.StringField(required=False)  # type: str
    followed_by_count = db.IntField(required=False)  # type: int
    follows_count = db.IntField(required=False)  # type: int
    media_count = db.IntField(required=False)  # type: int
    followed_by = db.ListField(db.StringField(), required=False)  # List of usernames
    follows = db.ListField(db.StringField(), required=False)  # type: list
    media = db.ListField(db.StringField(), required=False)  # type: list

    @classmethod
    def from_dict(cls, data_dict: dict, counts_dict: dict):
        data_dict['media_count'] = counts_dict['media']
        data_dict['follows_count'] = counts_dict['follows']
        data_dict['followed_by_count'] = counts_dict['followed_by']
        return super(User, cls).from_dict(data_dict)


class Taggee(db.EmbeddedDocument):
    user = db.ReferenceField(User)  # type: User
    position = db.EmbeddedDocumentField(Point)  # type: Point

    @classmethod
    def from_dict(cls, data_dict: dict):
        temp_user = User.from_dict(data_dict['user'], {})
        if User.objects.filter(pk=temp_user.pk).first() is None:
            temp_user.save()
        return super(Taggee, cls).from_dict()




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
    taggees = db.ListField(db.EmbeddedDocumentField(Taggee))  # type: list


class Token(db.Document):
    username = db.StringField(required=True)
    user_id = db.IntField(required=True, primary_key=True)
    code = db.StringField(required=True)
    access_token = db.StringField(required=True)
    timestamp = db.DateTimeField(default=datetime.now)
