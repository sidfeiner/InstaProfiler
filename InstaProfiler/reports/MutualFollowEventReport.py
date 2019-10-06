from datetime import datetime
from typing import Optional, Union, Tuple, Set, List, Sequence, Dict, TypeVar
import logging

import fire

from InstaProfiler.common.LoggerManager import LoggerManager
from InstaProfiler.common.MySQL import MySQLHelper
from InstaProfiler.common.base import Serializable, InstaUser
from InstaProfiler.reports.base import AsTableRow, HTMLTableConverter, AsGroupedTableRow
from InstaProfiler.reports.export.SendGridExporter import SendGridExporter
from InstaProfiler.scrapers.common.UserFollowsScraper import FOLLOW_TYPE_IDS

DATE_FORMAT = "%Y-%m-%d"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_MUTUAL_TIMEFRAME_DAYS = 3
MUTUAL_FOLLOW_TABLE_HEADERS = ["משתמש 2", "תאריך 1", "משתמש 2", "תאריך 2", "פעולה"]
TRENDING_FOLLOW_TABLE_HEADERS = ["חשבון מעניין", "כמות משתמשים פועלים", "תאריך ראשון", "תאריך אחרון", "פעולה",
                                 "משתמש פועל"]
T = TypeVar('T')  # Declare type variable


class UserEvent(Serializable):
    def __init__(self, user: InstaUser, ts: datetime, event_id: int):
        self.user = user
        self.ts = ts
        self.event_id = event_id


class MutualFollowEvent(Serializable):
    def __init__(self, user1: UserEvent, user2: UserEvent):
        assert user1.event_id == user2.event_id
        self.user1 = user1
        self.user2 = user2
        self.event_id = user1.event_id


class TrendingFollowEvent(Serializable):
    def __init__(self, src_users: List[str], dst_user: str, first_ts: datetime, last_ts: datetime,
                 event_id: int, count: int):
        self.src_users = src_users  # List of usernames
        self.count = count
        self.dst_user = dst_user
        self.first_ts = first_ts
        self.last_ts = last_ts
        self.event_id = event_id

    def __eq__(self, other):
        if not isinstance(other, MutualFollowEvent):
            return False
        other_users = [other.user1.user, other.user2.user]
        return self.user1.user in other_users and self.user2 in other_users and self.event_id == other.event_id

    def __hash__(self):
        min_user = min(self.user1.user.user_id, self.user2.user.user_id)
        max_user = max(self.user1.user.user_id, self.user2.user.user_id)
        str_num = "{}{}{}".format(self.event_id, min_user, max_user)
        return int(str_num)


class MutualFollowEventRow(AsTableRow):
    def __init__(self, user_name_1: str, time_1: str, user_name_2: str, time_2: str, event_name: str):
        self.user_name_1 = user_name_1
        self.time_1 = time_1
        self.user_name_2 = user_name_2
        self.time_2 = time_2
        self.event_name = event_name

    @classmethod
    def get_export_fields(cls) -> List[str]:
        return ["user_name_1", "time_1", "user_name_2", "time_2", "event_name"]

    @classmethod
    def from_mutual_follow_event(cls, event: MutualFollowEvent):
        event_name = "follow" if event.event_id == FOLLOW_TYPE_IDS.FOLLOW else "unfollow"
        return MutualFollowEventRow(
            event.user1.user.username, event.user1.ts.strftime(DATE_TIME_FORMAT),
            event.user2.user.username, event.user2.ts.strftime(DATE_TIME_FORMAT),
            event_name
        )


class TrendingFollowEventRow(AsGroupedTableRow):
    def __init__(self, src_user_name: str, dst_user_name: str, cnt: int, first_ts: str, last_ts: str, event_name: str):
        self.src_user_name = src_user_name
        self.dst_user_name = dst_user_name
        self.cnt = cnt
        self.first_ts = first_ts
        self.last_ts = last_ts
        self.event_name = event_name

    @classmethod
    def get_export_fields(cls) -> List[str]:
        return ["dst_user_name", "src_user_name", "cnt", "first_ts", "last_ts", "event_name"]

    @classmethod
    def get_group_by_fields(cls) -> List[str]:
        return ["dst_user_name", "cnt", "first_ts", "last_ts", "event_name"]

    @classmethod
    def from_trending_follow_event(cls, event: TrendingFollowEvent) -> List['TrendingFollowEventRow']:
        event_name = "follow" if event.event_id == FOLLOW_TYPE_IDS.FOLLOW else "unfollow"
        return [TrendingFollowEventRow(
            src_user, event.dst_user, event.count, event.first_ts.strftime(DATE_TIME_FORMAT),
            event.last_ts.strftime(DATE_TIME_FORMAT), event_name
        ) for src_user in event.src_users]


class MutualFollowEventReport(object):
    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        LoggerManager.init(log_path, level=log_level, with_console=log_to_console)
        self.logger = LoggerManager.get_logger(__name__)

    @staticmethod
    def build_ts_filter(from_date: Optional[datetime], to_date: Optional[datetime],
                        days_back: Optional[int] = None, ts_col: str = "ts") -> Tuple[str, Optional[list]]:
        assert from_date is not None or to_date is not None or days_back is not None
        if days_back is not None:
            return "{0} > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL {1} DAY)".format(ts_col, days_back), []
        if from_date is not None and to_date is not None:
            return "{0} between ? and ?".format(ts_col), [from_date, to_date]
        # If code here is reached, either from_date or to_date are not None
        if from_date is not None:
            return "{0} >= ?".format(ts_col), [from_date]
        else:
            return "{0} <= ?".format(ts_col), [to_date]

    def find_mutual_event_type(self, mysql: MySQLHelper, from_date: Optional[datetime], to_date: Optional[datetime],
                               mutual_event_timeframe_days: int, days_back: Optional[int] = None) -> Set[
        MutualFollowEvent]:
        """Find users that have both followed each other or unfollowed each other

        :param from_date: from_date to query from
        :param to_date: to_date to query
        :param days_back: If given, ignore from_date/to_date
        :param mutual_event_timeframe_days: Maximum amount of days for it to be considered a mutual event type
                                            For example, mutual unfollow is only if they have both unfollowed each other
                                            in the past 2 days.
        """
        ts_filter_1, params_1 = self.build_ts_filter(from_date, to_date, days_back, ts_col="fe1.ts")
        ts_filter_2, params_2 = self.build_ts_filter(from_date, to_date, days_back, ts_col="fe2.ts")
        ts_filter = "({}) and ({})".format(ts_filter_1, ts_filter_2)
        params = params_1 + params_2
        sql = """
                select fe1.src_user_name as user_name_1,
                       fe1.src_user_id as user_id_1,
                       fe2.src_user_name as user_name_2,
                       fe2.src_user_id as user_id_2,
                       fe1.ts as user_1_event_ts,
                       fe2.ts as user_2_event_ts,
                       fe1.follow_type_id as follow_type_id,
                       abs(timestampdiff(day, fe1.ts, fe2.ts)) as day_diff
                from follow_events fe1
                         join follow_events fe2 on fe1.dst_user_id = fe2.src_user_id
                    and fe1.src_user_id = fe2.dst_user_id and fe1.follow_type_id = fe2.follow_type_id
                    and (fe1.src_user_name = 'edenamsalem1_' or fe2.src_user_name = 'edenamsalem1_')
                where {ts_filter} and abs(timestampdiff(day, fe1.ts, fe2.ts)) < ?
        """.format(ts_filter=ts_filter)

        params.append(mutual_event_timeframe_days)
        mutual_events_records = mysql.query(sql, params)
        events = set()
        for row in mutual_events_records:
            mutual_event = MutualFollowEvent(
                UserEvent(InstaUser(row.user_id_1, row.user_name_1), row.user_1_event_ts, row.follow_type_id),
                UserEvent(InstaUser(row.user_id_2, row.user_name_2), row.user_2_event_ts, row.follow_type_id)
            )
            events.add(mutual_event)
        return events

    def find_trending_event_type(self, mysql: MySQLHelper, from_date: Optional[datetime], to_date: Optional[datetime],
                                 days_back: Optional[int] = None) -> List[
        TrendingFollowEvent]:
        """Find users that have both followed each other or unfollowed each other

        :param from_date: from_date to query from
        :param to_date: to_date to query
        :param days_back: If given, ignore from_date/to_date
        """
        ts_filter, params = self.build_ts_filter(from_date, to_date, days_back)
        sql = """
            select dst_user_name,
                   follow_type_id,
                   min(ts)                     as first_ts,
                   max(ts)                     as last_ts,
                   count(*)                       cnt,
                   group_concat(src_user_name) as users
            from follow_events
            where {ts_filter}
            group by dst_user_name, follow_type_id
            having count(*) > 50
            order by cnt desc;
        """.format(ts_filter=ts_filter)

        trending_events_records = mysql.query(sql, params)
        events = [
            TrendingFollowEvent(row.users.split(','), row.dst_user_name, row.first_ts, row.last_ts, row.follow_type_id,
                                row.cnt)
            for row in trending_events_records]
        return events

    def prepare_mail(self, mutual_follow_events: Sequence[MutualFollowEvent],
                     mutual_unfollow_events: Sequence[MutualFollowEvent],
                     trending_follow_events: Sequence[TrendingFollowEvent],
                     trending_unfollow_events: Sequence[TrendingFollowEvent]):
        rows = ["<h3>גללתי ומצאתי!!</h3>"]
        if len(mutual_follow_events) > 0:
            mutual_follow_event_rows = [MutualFollowEventRow.from_mutual_follow_event(x) for x in mutual_follow_events]
            follow_table = HTMLTableConverter.to_table(mutual_follow_event_rows, headers=MUTUAL_FOLLOW_TABLE_HEADERS)
            rows.append("<h5>אנשים שהתחילו לעקוב אחד אחרי השני:</h5>")
            rows.append(follow_table)
        if len(mutual_unfollow_events) > 0:
            mutual_unfollow_event_rows = [MutualFollowEventRow.from_mutual_follow_event(x) for x in
                                          mutual_unfollow_events]
            unfollow_table = HTMLTableConverter.to_table(mutual_unfollow_event_rows,
                                                         headers=MUTUAL_FOLLOW_TABLE_HEADERS)
            rows.append("<h5>אנשים שהסירו עוקב אחד מהשני:</h5>")
            rows.append(unfollow_table)
        if len(trending_follow_events) > 0:
            trending_follow_event_rows = [event for x
                                          in trending_follow_events for event in
                                          TrendingFollowEventRow.from_trending_follow_event(x)]
            follow_table = HTMLTableConverter.to_grouped_table(trending_follow_event_rows,
                                                               TRENDING_FOLLOW_TABLE_HEADERS)
            rows.append("<h5>אנשים שעוקבים לפי טרנד מסויים:</h5>")
            rows.append(follow_table)
        if len(trending_unfollow_events) > 0:
            trending_unfollow_event_rows = [event for x
                                            in trending_unfollow_events for event in
                                            TrendingFollowEventRow.from_trending_follow_event(x)]
            unfollow_table = HTMLTableConverter.to_grouped_table(trending_unfollow_event_rows,
                                                                 TRENDING_FOLLOW_TABLE_HEADERS)
            rows.append("<h5>אנשים שהסירו עוקב לפי טרנד מסויים:</h5>")
            rows.append(unfollow_table)

        msg = """
        <div dir="rtl">
        {0}
        </div>
        """.format('<br />'.join(rows))
        return msg

    def group_by_event_id(self, events: List[T]) -> Tuple[List[T], List[T]]:
        """Split events and return list of events with follows and then with unfollows"""
        follow_events = []
        unfollow_events = []
        for event in events:
            if event.event_id == FOLLOW_TYPE_IDS.FOLLOW:
                follow_events.append(event)
            elif event.event_id == FOLLOW_TYPE_IDS.UNFOLLOW:
                unfollow_events.append(event)
            else:
                raise Exception("unknown follow type reached")
        return follow_events, unfollow_events

    def get_mutual_follow_events(self, mysql: MySQLHelper, from_date: Optional[datetime], to_date: Optional[datetime],
                                 mutual_event_timeframe_days: Optional[int], days_back: Optional[int]) -> Tuple[
        List[MutualFollowEvent], List[MutualFollowEvent]]:
        # mutual_events = self.find_mutual_event_type(mysql, from_date, to_date, mutual_event_timeframe_days, days_back)

        mutual_events = (
            MutualFollowEvent(UserEvent(InstaUser(1, "1", ), datetime.now(), 1),
                              UserEvent(InstaUser(2, "2"), datetime.now(), 1)),
            MutualFollowEvent(UserEvent(InstaUser(2, "2", ), datetime.now(), 1),
                              UserEvent(InstaUser(1, "1"), datetime.now(), 1))
        )
        return self.group_by_event_id(mutual_events)

    def get_trending_follow_events(self, mysql: MySQLHelper, from_date: Optional[datetime], to_date: Optional[datetime],
                                   days_back: Optional[int]):
        trending_events = self.find_trending_event_type(mysql, from_date, to_date,
                                                        days_back)
        return self.group_by_event_id(trending_events)

    def main(self, from_date: Optional[str] = None, to_date: Optional[str] = None, days_back: Optional[int] = None,
             mutual_event_timeframe_days: int = DEFAULT_MUTUAL_TIMEFRAME_DAYS):
        assert from_date is not None or to_date is not None or days_back is not None
        if to_date is not None:
            to_date = datetime.strptime(to_date, DATE_FORMAT)
        if from_date is not None:
            from_date = datetime.strptime(from_date, DATE_FORMAT)
        mysql = MySQLHelper('mysql-insta-local')

        mutual_follow_events, mutual_unfollow_events = self.get_mutual_follow_events(mysql, from_date, to_date,
                                                                                     mutual_event_timeframe_days,
                                                                                     days_back)
        trending_follow_events, trending_unfollow_events = self.get_trending_follow_events(mysql, from_date, to_date,
                                                                                           days_back)

        msg = self.prepare_mail(mutual_follow_events, mutual_unfollow_events, trending_follow_events,
                                trending_unfollow_events)

        self.logger.info("Exporting email")
        exporter = SendGridExporter()
        resp = exporter.send_email('sidfeiner@gmail.com', ['sidfeiner@gmail.com'], 'גללתי ומצאתי - אחד על אחד',
                                   html_content=msg)

        self.logger.info("Done exporting.")


if __name__ == '__main__':
    fire.Fire(MutualFollowEventReport)
