#-*- encoding: utf-8 -*-
import requests
from conf.flask import Media, User, Like, Comment, Location, Point, Taggee, Token
from typing import List

__author__ = 'Sidney'

class OAuthException(Exception):
    def __init__(self, type, msg):
        super(OAuthException, self).__init__(msg)
        self.type = type

def get_access_token(redirect_url: str, code: str) -> Token:
    """
    :param redirect_url: URL to redirect to
    :type redirect_url: str
    :param code: code received from the login
    :type code: str
    :return: Full token
    :rtype: Token
    """
    data = {
        "client_id": "1ce2ad36a097486984642c7d6db041ed",
        "client_secret": "864932c554f342628a61913d4d3c8406",
        "grant_type": "authorization_code",
        "redirect_uri": redirect_url,
        "code": code
    }
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


def get_medias(user_id, access_token: str) -> List[Media]:
    """
    :param user_id: User's ID that we want to get the medias from
    :param access_token: Instagram Access Token
    :return: List of media from that user
    """
    pass

