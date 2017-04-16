#-*- encoding: utf-8 -*-
import requests
from conf import constants
from conf.flask import Media, User, Like, Comment, Location, Point, Taggee, Token
from typing import List
from LoggerManager import logger

__author__ = 'Sidney'

class OAuthException(Exception):
    def __init__(self, type, msg):
        super(OAuthException, self).__init__(msg)
        self.type = type


API_URL = "https://api.instagram.com"
VERSION = "v1"
USERS_ENDPOINT = "users"

def get_access_token(redirect_url: str, code: str) -> Token:
    """
    :param redirect_url: URL to redirect to
    :param code: code received from the login
    :return: Full token
    """
    encoded_url = redirect_url.split("?")[0]
    data = {
        "client_id": constants.client_id,
        "client_secret": constants.client_secret,
        "grant_type": "authorization_code",
        "redirect_uri": encoded_url,
        "code": code
    }
    logger.info("sent access token request with redirect_url: {0}".format(encoded_url))
    resp = requests.post("https://api.instagram.com/oauth/access_token", data)

    json_resp = resp.json()  # type: dict
    if 'error_type' in json_resp.keys():
        raise OAuthException(
            json_resp.get("error_type", ""),
            json_resp.get("error_message", "")
        )
    access_token = json_resp['access_token']
    user = json_resp['user']
    user_id = user['id']
    username = user['username']
    return Token(access_token=access_token, user_id=user_id, username=username, code=code)


def enrich_user(user: User, access_token: str) -> User:
    """
    :param user: user's ID
    :param access_token: Access Token given by instagram
    :return: User with all relevant infos
    """
    url = "{domain}/{version}/{path}/{id}?access_token={token}".format(
        domain=API_URL,
        version=VERSION,
        path=USERS_ENDPOINT,
        id=user.user_id,
        token=access_token
    )
    response_json = requests.get(url).json()['data']

    user.user_name = response_json['username']
    user.full_name = response_json['full_name']
    user.bio = response_json['bio']
    user.website = response_json['website']

    counts = response_json['counts']
    user.media_count = counts['media']
    user.follows_count = counts['follows']
    user.followed_by_count = counts['followed_by']

    return user


def get_medias(user_id, access_token: str) -> List[Media]:
    """
    :param user_id: User's ID that we want to get the medias from
    :param access_token: Instagram Access Token
    :return: List of media from that user
    """
    pass

user = User(user_id='31457967', user_name="sid802", full_name="Sidney Feiner")
print(user.followed_by_count)
enrich_user(user, "31457967.81c816e.0d2576dde14943b5ab91d02d8a9b0f02")
print(user.followed_by_count)