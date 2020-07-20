import logging
import os
from json.decoder import JSONDecodeError
from time import sleep
import json
from typing import Optional, Union, Callable
import pickle
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.remote.webdriver import WebDriver
from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.base import InstaUser

CHROME_DRIVER_PATH = '/usr/bin/chromedriver'
LOGIN_BTN_XPATH = '//button[contains(text(), "Log in")]'
LOGIN_USER_INPUT_XPATH = '//div[@id="email_container"]/input'
LOGIN_PWD_INPUT_XPATH = '//input[@type="password"]'
LOGIN_SUBMIT_INPUT_XPATH = '//button[@name="login"]'
LOGIN_AS_USER_BTN_XPATH = '//div[contains(text(), "Continue as")]'

DEFAULT_MAIL = 'sidfeiner@gmail.com'
DEFAULT_USER_NAME = 'sidfeiner'
DEFAULT_PWD = 'SidAtFB803'
DEFAULT_COOKIES_PKL_FILE = '/opt/InstaProfiler/InstaProfiler/cookies.pkl'


class QueryHashes:
    # Different for every account
    FOLLOWERS = "56066f031e6239f35a904ac20c9f37d9"
    FOLLOWING = "c56ee0ae1f89cdbd1c89e2bc6b8f3d18"
    STORY_VIEWERS = 'de8017ee0a7c9c45ec4260733d81ea31'
    MEDIA = "f2405b236d85e8296cf30347c9f08c2a"
    MEDIA_LIKES = "d5d763b1e2acf209d62d22d184488e57"


class MaxRetriesReached(Exception):
    pass


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

    def parse_current_user_info(self) -> (str, str):
        """Parse currently logged on user id and username"""
        user_id = self.driver.execute_script('return window._sharedData.config.viewer.id')
        user_name = self.driver.execute_script('return window._sharedData.config.viewer.username')
        return user_id, user_name

    def _is_logged_in(self) -> bool:
        """Returns if a user is logged in"""
        return ' logged-in' in self.driver.find_element_by_xpath('html').get_attribute('class')

    def _save_cookies(self, cookies_pkl_path: str):
        """Save cookies to pkl files"""
        self.logger.info("Saving cookies to pkl file")
        cookies = self.driver.get_cookies()
        with open(cookies_pkl_path, 'wb') as fp:
            pickle.dump(cookies, fp)

    def login(self, user: Optional[str], password: Optional[str], cookies_pkl_path: Optional[str]) -> bool:
        """Logs in. first tries with cookies and then with basic auth"""
        logged_in = False
        if cookies_pkl_path is not None and os.path.exists(cookies_pkl_path):
            logged_in = self.login_with_cookies(cookies_pkl_path)
        if user is not None and password is not None and not logged_in:
            logged_in = self.login_with_auth(user, password)
            if logged_in and cookies_pkl_path:
                self._save_cookies(cookies_pkl_path)
        return logged_in

    def login_with_cookies(self, cookies_pkl_path: str) -> bool:
        self.logger.info("Logging in with cookies pickle file")

        self.logger.info("Navigating to domain other than instagram...")
        self.driver.get('http://www.google.com')

        with open(cookies_pkl_path, 'rb') as fp:
            cookies = pickle.load(fp)
        for cookie in cookies:
            if 'expiry' in cookie:
                cookie.pop('expiry')
            self.driver.add_cookie(cookie)

        self.logger.info("Returning to original url")
        self.to_home_page()
        return self._is_logged_in()

    def login_with_auth(self, user: str, password: str):
        self.logger.info("Logging in...")
        self.to_home_page()
        sleep(2)
        self.driver.find_element_by_xpath(LOGIN_BTN_XPATH).click()
        self.driver.find_element_by_xpath(LOGIN_USER_INPUT_XPATH).send_keys(user)
        self.driver.find_element_by_xpath(LOGIN_PWD_INPUT_XPATH).send_keys(password)
        self.driver.find_element_by_xpath(LOGIN_SUBMIT_INPUT_XPATH).click()
        sleep(3)
        els = self.driver.find_elements_by_xpath(LOGIN_AS_USER_BTN_XPATH)
        if len(els) > 0:
            els[0].click()
        sleep(3)
        if self._is_logged_in():
            self.logger.info('done logging in. waiting 3 seconds...')
            return True
        else:
            self.logger.warn('Failed logging with username and pwd')
            return False

    def to_home_page(self):
        self.driver.get(self.INSTA_URL)
        sleep(1)

    def get_url_data(self, url: str, is_valid_func: Callable[[str, ], bool], max_allowed_retries: int = 120,
                     wait_seconds: int = 60):
        retries = 0
        self.driver.get(url)
        data = self.driver.find_element_by_tag_name('body').text
        while not is_valid_func(data) and retries < max_allowed_retries:
            self.logger.warning("Waiting %d seconds", wait_seconds)
            sleep(wait_seconds)
            retries += 1
            self.driver.get(url)
            data = self.driver.find_element_by_tag_name('body').text

        if retries == max_allowed_retries:
            raise MaxRetriesReached()
        return data

    def get_url_json_data(self, url: str, is_valid_func: Callable[[dict, ], bool], max_allowed_retries: int = 120,
                     wait_seconds: int = 60):
        data = self.get_url_data(url, lambda x: is_valid_func(json.loads(x)), max_allowed_retries, wait_seconds)
        return json.loads(data)

    def scrape_user(self, user_name: str, driver: Optional[WebDriver] = None,
                    max_retries: int = 100, wait_seconds: int = 150) -> Optional[InstaUser]:
        """Scrapes InstaUser data. Returns None if user doesn't exist"""
        self.logger.info('Scraping user data for %s', user_name)
        if driver is None:
            self.init_driver()
            driver = self.driver
        body = self.get_url_data('{0}/{1}/?__a=1'.format(self.INSTA_URL, user_name), lambda x: 'minutes' not in x,
                                 max_retries, wait_seconds)
        try:
            user_data = json.loads(body)['graphql']['user']
            return InstaUser.from_dict(user_data)
        except KeyError as e:
            return None
        except JSONDecodeError as e:
            return None
        except Exception as e:
            self.logger.error("Failed parsing following: %s", driver.find_element_by_tag_name('body').text)
            raise e

    def init_driver(self):
        if self.driver is None:
            self.logger.info('Initing driver...')
            opts = ChromeOptions()
            opts.add_argument('headless')
            opts.add_argument('--no-sandbox')
            opts.add_argument('--disable-dev-shm-usage')
            self.driver = Chrome(executable_path=CHROME_DRIVER_PATH, chrome_options=opts)
            self.login(DEFAULT_MAIL, DEFAULT_PWD, DEFAULT_COOKIES_PKL_FILE)
