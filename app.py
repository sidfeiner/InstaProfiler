#-*- encoding: utf-8 -*-
from flask import Flask, request
from flask_mongoengine import MongoEngine
from conf import constants
import insta_api

__author__ = 'Sidney'

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
app.run(port=constants.flask_port)


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


@app.route('/auth')
def auth():
    # redirected from: https://api.instagram.com/oauth/authorize/?client_id=1ce2ad36a097486984642c7d6db041ed&redirect_uri=http://localhost:5000/auth&scope=basic+follower_list+comments+relationships+public_content+likes&response_type=code
    return insta_api.get_access_token(code=request.args.get('code'), redirect_url=request.url)
