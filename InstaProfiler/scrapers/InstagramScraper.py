import logging
from time import sleep
import json
from typing import Optional, Union

from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.remote.webdriver import WebDriver
from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.base import InstaUser

CHROME_DRIVER_PATH = '/home/sid/miniconda3/bin/chromedriver'
LOGIN_BTN_XPATH = '//button[contains(text(), "Log in")]'
LOGIN_USER_INPUT_XPATH = '//input[@id="email"]'
LOGIN_PWD_INPUT_XPATH = '//input[@id="pass"]'
LOGIN_SUBMIT_INPUT_XPATH = '//button[@id="loginbutton"]'

DEFAULT_MAIL = 'sidfeiner@gmail.com'
DEFAULT_USER_NAME = 'sidfeiner'
DEFAULT_PWD = 'Qraaynem802'


class QueryHashes:
    # Different for every account
    FOLLOWERS = "56066f031e6239f35a904ac20c9f37d9"
    FOLLOWING = "c56ee0ae1f89cdbd1c89e2bc6b8f3d18"
    STORY_VIEWERS = 'de8017ee0a7c9c45ec4260733d81ea31'


class InstagramScraper(object):
    def __init__(self, log_path: Optional[str], log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        LoggerManager.init(log_path, level=log_level, with_console=log_to_console)
        self.logger = LoggerManager.get_logger(__name__)
        self.driver = None  # type: WebDriver

    INSTA_URL = "https://www.instagram.com"
    GRAPH_URL = "{0}/graphql/query".format(INSTA_URL)

    @classmethod
    def parse_user_id_from_profile(cls, driver: WebDriver) -> str:
        """Return profile's user_id"""
        user_id = driver.execute_script('return window._sharedData.entry_data.ProfilePage[0].graphql.user.id')
        return user_id

    def parse_current_user_id(self) -> str:
        """Parse currently logged on user id"""
        user_id = self.driver.execute_script('return window._sharedData.config.viewer.id')
        return user_id

    def login(self, user: str, password: str):
        self.logger.info("Logging in...")
        self.to_home_page()
        sleep(2)
        self.driver.find_element_by_xpath(LOGIN_BTN_XPATH).click()
        self.driver.find_element_by_xpath(LOGIN_USER_INPUT_XPATH).send_keys(user)
        self.driver.find_element_by_xpath(LOGIN_PWD_INPUT_XPATH).send_keys(password)
        self.driver.find_element_by_xpath(LOGIN_SUBMIT_INPUT_XPATH).click()
        self.logger.info('done logging in. waiting 3 seconds...')
        sleep(3)

    def to_home_page(self):
        self.driver.get(self.INSTA_URL)
        sleep(1)

    def scrape_user(cls, driver: WebDriver, user_name: str):
        cls.logger.info('Scraping user data for %s', user_name)
        driver.get('{0}/{1}/?__a=1'.format(cls.INSTA_URL, user_name))
        user_data = json.loads(driver.find_element_by_tag_name('body').text)['graphql']['user']
        return InstaUser.from_dict(user_data)

    def init_driver(self):
        if self.driver is None:
            self.logger.info('Initing driver...')
            opts = ChromeOptions()
            opts.add_argument('headless')
            self.driver = Chrome(executable_path=CHROME_DRIVER_PATH, chrome_options=opts)
            self.login(DEFAULT_MAIL, DEFAULT_PWD)
