from time import sleep
import json
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.remote.webdriver import WebDriver
from common import LoggerManager
from common.base import InstaUser

CHROME_DRIVER_PATH = '/home/sid/personal/Projects/EMIReports/configs/lin/chromedriver'
LOGIN_BTN_XPATH = '//button[contains(text(), "Log in")]'
LOGIN_USER_INPUT_XPATH = '//input[@id="email"]'
LOGIN_PWD_INPUT_XPATH = '//input[@id="pass"]'
LOGIN_SUBMIT_INPUT_XPATH = '//button[@id="loginbutton"]'

DEFAULT_USERNAME = 'sidfeiner@gmail.com'
DEFAULT_PWD = 'Qraaynem802'


class QueryHashes:
    # Different for every account
    FOLLOWERS = "56066f031e6239f35a904ac20c9f37d9"
    FOLLOWING = "c56ee0ae1f89cdbd1c89e2bc6b8f3d18"
    STORY_VIEWERS = 'de8017ee0a7c9c45ec4260733d81ea31'


class InstagramScraper(object):
    driver = None  # type: WebDriver
    logger = LoggerManager.logger

    INSTA_URL = "https://www.instagram.com"
    GRAPH_URL = "{0}/graphql/query".format(INSTA_URL)

    @classmethod
    def parse_user_id_from_profile(cls, driver: WebDriver) -> str:
        """Return profile's user_id"""
        user_id = driver.execute_script('return window._sharedData.entry_data.ProfilePage[0].graphql.user.id')
        return user_id

    @classmethod
    def parse_current_user_id(cls) -> str:
        """Parse currently logged on user id"""
        user_id = cls.driver.execute_script('return window._sharedData.config.viewer.id')
        return user_id

    @classmethod
    def login(cls, driver: WebDriver, user: str, password: str):
        cls.logger.info("Logging in...")
        cls.to_home_page()
        sleep(2)
        driver.find_element_by_xpath(LOGIN_BTN_XPATH).click()
        driver.find_element_by_xpath(LOGIN_USER_INPUT_XPATH).send_keys(user)
        driver.find_element_by_xpath(LOGIN_PWD_INPUT_XPATH).send_keys(password)
        driver.find_element_by_xpath(LOGIN_SUBMIT_INPUT_XPATH).click()
        cls.logger.info('done logging in. waiting 3 seconds...')
        sleep(3)

    @classmethod
    def to_home_page(cls):
        cls.driver.get(cls.INSTA_URL)
        sleep(1)

    @classmethod
    def scrape_user(cls, driver: WebDriver, user_name: str):
        cls.logger.info('Scraping user data for %s', user_name)
        driver.get('{0}/{1}/?__a=1'.format(cls.INSTA_URL, user_name))
        user_data = json.loads(driver.find_element_by_tag_name('body').text)['graphql']['user']
        return InstaUser.from_dict(user_data)

    @classmethod
    def init_driver(cls):
        if cls.driver is None:
            cls.logger.info('Initing driver...')
            opts = ChromeOptions()
            opts.add_argument('headless')
            cls.driver = Chrome(executable_path=CHROME_DRIVER_PATH, chrome_options=opts)
            cls.login(cls.driver, DEFAULT_USERNAME, DEFAULT_PWD)
