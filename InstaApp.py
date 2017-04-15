#-*- encoding: utf-8 -*-
from flask import request, jsonify
from conf import constants
from conf.flask import app
import insta_api
from LoggerManager import logger

__author__ = 'Sidney'


@app.route('/auth')
def auth():
    code = request.args.get('code')
    # redirected from: https://api.instagram.com/oauth/authorize/?client_id=1ce2ad36a097486984642c7d6db041ed&redirect_uri=http://ec2-54-71-98-189.us-west-2.compute.amazonaws.com:9000/auth&scope=basic+follower_list+comments+relationships+public_content+likes&response_type=code
    try:
        token = insta_api.get_access_token(code=code, redirect_url=request.url)
        token.modify(
            query={
                "user_id": token.user_id,
                "username": token.username
            }
        )
        logger.info("new auth: code: {0}, token: {1}".format(code, token.access_token))
        return token.access_token
    except insta_api.OAuthException as e:
        return jsonify({"error": e.type, "message": str(e)})

app.run(host=constants.flask_host, port=constants.flask_port)