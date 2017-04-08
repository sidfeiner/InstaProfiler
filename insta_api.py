#-*- encoding: utf-8 -*-
import requests

__author__ = 'Sidney'

def get_access_token(redirect_url, code):
    """
    :param redirect_url: URL to redirect to
    :type redirect_url: str
    :param code: code received from the login
    :type code: str
    :return: access token
    :rtype: str
    """
    data = {
        "client_id": "1ce2ad36a097486984642c7d6db041ed",
        "client_secret": "864932c554f342628a61913d4d3c8406",
        "grant_type": "authorization_code",
        "redirect_uri": redirect_url,
        "code": code
    }
    resp = requests.post("https://api.instagram.com/oauth/access_token", data)
    return resp['access_token']
