#-*- encoding: utf-8 -*-
__author__ = 'Sidney'
from conf.flask import Token


def save_token(token: Token) -> bool:
    """
    :param token: Token to upsert
    :return: True if new, False if updated
    """
    result = False
    if Token.objects.filter(pk=token.user_id).first() is None:
        # Doesn't exist yet
        token.save()
        result = True
    else:
        # Update only accesstoken and timestamap
        token.update(access_token=token.access_token, timestamp=token.timestamp)
    return result
