#-*-encoding: utf-8 -*-
import logging
from conf import constants
import sys
import os
from InstaApp import app
from datetime import datetime

__author__ = "Sidney"

logs_dir = "/.logs" if 'win' in sys.platform.lower() else constants.logging_dir_prod


def init_file_handler(logger: logging.Logger, encoding="utf-8"):
    """
    Append a file handler to the given logger
    """
    file_name = "{name}-{date}".format(name=app.name, date=datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    file_path = os.path.join(logs_dir, file_name)
    handler = logging.FileHandler(file_path, encoding=encoding)
    handler.setFormatter(format)
    logger.addHandler(handler)

logger = logging.getLogger("InstaProfiler")
format = logging.Formatter("%(asctime)s | %(levelno)s | $(module)s | %(lineno)s | %(message)s")
init_file_handler(logger)
