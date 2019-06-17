from InstaProfiler.scrapers.InstagramScraper import InstagramScraper
import fire


class UserNameToIdParser(InstagramScraper):
    def main(self, *user_names: str):
        self.init_driver()
        users = {}
        for user_name in user_names:
            self.logger.info("Handling user %s", user_name)
            user = self.scrape_user(self.driver, user_name)
            users[user_name] = user

        for user in users.values():
            print("(2, 'race-competitors', {0}, '{1}'),".format(user.user_id, user.username))

        return users


if __name__ == '__main__':
    fire.Fire(UserNameToIdParser)
