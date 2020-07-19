import re
from datetime import datetime

import fire

from InstaProfiler.common.MySQL import MySQLHelper

LOG_RECORD_REGEX = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\s:\s(?P<name>\w+):\s(?P<level>\w+)\s:\s(?P<class>[\w.]+)\s:\s(?P<logger>[\w.]+)\s:\s(?P<msg>.+?)(?:\n\d{4})",
    re.DOTALL)
UNFOLLOW_PARAMS_REGEX = re.compile(
    r"""datetime\((?P<year>\d+),\s(?P<month>\d+),\s(?P<day>\d+),\s(?P<hour>\d+),\s(?P<minute>\d+),\s(?P<second>\d+),\s(?P<frac>\d+)\),\s'(?P<src_user>[\w.]+)'""")
UNFOLLOW_USERS_REGEX = re.compile(r"""dst_user_id in \((?P<users>.+?)\)""")


class LogParser(object):
    @staticmethod
    def main():
        with open('/opt/InstaProfiler/logs/user-follows.log') as fp:
            txt = fp.read()
        matches = LOG_RECORD_REGEX.findall(txt)
        all_queries = []
        unfollow_users_distinct = set()
        for match in matches:
            if 'UPDATE follows' in match[5]:
                unfollow_params = UNFOLLOW_PARAMS_REGEX.search(match[5])
                ts_params = [unfollow_params.group('year'), unfollow_params.group('month'),
                             unfollow_params.group('day'), unfollow_params.group('hour'),
                             unfollow_params.group('minute'), unfollow_params.group('second'),
                             unfollow_params.group('frac')]
                unfollow_ts = datetime(*[int(x) for x in ts_params])
                src_user = unfollow_params.group('src_user')
                unfollow_users = UNFOLLOW_USERS_REGEX.search(match[5]).group('users').split(', ')
                if len(unfollow_users) < 8:
                    for u in unfollow_users:
                        unfollow_users_distinct.add(u.strip("'"))
                    query = "UPDATE follows set dst_follows=0, dst_unfollows_latest_timestamp=? where src_user_name=? and dst_user_id in ({users})".format(users=','.join(unfollow_users))
                    params = (unfollow_ts, src_user)
                    print(query)
                    all_queries.append((query, params))
                else:
                    print("Too much users", len(unfollow_users))

        print(','.join(unfollow_users_distinct))
        odbc_helper = MySQLHelper('mysql-insta-local')
        cursor = odbc_helper.get_cursor()
        for query in all_queries:
            odbc_helper.execute(query[0], query[1], cursor)
        odbc_helper.commit()
        odbc_helper.close()



if __name__ == '__main__':
    fire.Fire(LogParser.main)
