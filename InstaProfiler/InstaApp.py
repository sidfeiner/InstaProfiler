#-*- encoding: utf-8 -*-
from flask import request, jsonify
from InstaProfiler.conf import constants
from InstaProfiler.conf.flask import app
from InstaProfiler import insta_api
from InstaProfiler.common.LoggerManager import LoggerManager

__author__ = 'Sidney'

logger = LoggerManager.get_logger('InstaApp')

@app.route('/auth')
def auth():
    code = request.args.get('code')
    # redirected from: https://api.instagram.com/oauth/authorize/?client_id=81c816e7c7414edcb82b19a9f40867c1&redirect_uri=http%3A%2F%2Fec2-54-71-98-189.us-west-2.compute.amazonaws.com%3A9000%2Fauth&scope=likes+comments&response_type=code
    try:
        token = insta_api.get_access_token(code=code, redirect_url=request.url)
        token.save()
        #MongoDao.save_token(token)
        logger.info("new auth: code: {0}, token: {1}".format(code, token.access_token))
        return token.access_token
    except insta_api.OAuthException as e:
        return jsonify({
            "error": e.type,
            "message": str(e)
        })


app.run(host=constants.flask_host, port=constants.flask_port)