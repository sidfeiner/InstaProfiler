import logging
from typing import Optional, Union


class LoggerManager(object):
    logger = None  # type: logging.Logger
    is_init = False
    loggers = {}

    @classmethod
    def init_console_handler(cls, formatter: logging.Formatter):
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        return handler

    @classmethod
    def init(cls, file_path: Optional[str] = "./job.log", logger_name: Optional[str] = None, level: Union[int, str] = logging.DEBUG,
             format_str: str = "%(asctime)s : %(threadName)s: %(levelname)s : %(name)s : %(module)s : %(message)s", with_console: bool = True):
        if not cls.is_init:
            logger = logging.getLogger(logger_name or "BaseLogger")
            logger.setLevel(level)
            formatter = logging.Formatter(format_str)

            if file_path is not None:
                file_handler = logging.FileHandler(file_path, encoding='utf-8')
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

            if with_console:
                logger.addHandler(cls.init_console_handler(formatter))

            logger.propagate = False

            cls.logger = logger
            cls.is_init = True

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        cls.init()
        logger = cls.logger.getChild(name)  # type: logging.Logger
        cls.loggers[name] = logger
        return logger