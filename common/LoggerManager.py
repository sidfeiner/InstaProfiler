#-*-encoding: utf-8 -*-
import logging
from conf import constants
import sys
import os
from conf.flask import app
from datetime import datetime

__author__ = "Sidney"

logs_dir = "./logs" if 'win' in sys.platform.lower() else constants.logging_dir_prod


def init_file_handler(logger: logging.Logger, encoding="utf-8"):
    """
    Append a file handler to the given logger
    """
    file_name = "{name}-{date}".format(name=app.name, date=datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    file_path = os.path.join(logs_dir, file_name)
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    handler = logging.FileHandler(file_path, encoding=encoding)
    handler.setFormatter(log_format)
    logger.addHandler(handler)

def init_console_handler(logger: logging.Logger):
    handler = logging.StreamHandler()
    handler.setFormatter(log_format)
    logger.addHandler(handler)

logger = logging.getLogger("InstaProfiler")
logger.setLevel(logging.DEBUG)
log_format = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(lineno)s | %(message)s")
print("Initiating handler in dir: {0}".format(os.path.abspath(logs_dir)))
init_file_handler(logger)
init_console_handler(logger)
#logger.propagate = False
print("Handler initiated")