from datetime import datetime
from typing import Optional, Union, Tuple, Set, List, Sequence, Dict, TypeVar
import logging

import fire

from InstaProfiler.common.MySQL import MySQLHelper
from InstaProfiler.reports.base import AsTableRow, HTMLTableConverter, AsGroupedTableRow, Report, HTMLImage
from InstaProfiler.reports.export.SendGridExporter import SendGridExporter
from InstaProfiler.scrapers.common.media.MediaScraper import MediaRecord
from InstaProfiler.scrapers.common.media.base import MediaTypes

DATE_FORMAT = "%Y-%m-%d"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
NEW_MEDIA_TABLE_HEADERS = ["משתמש", "תאריך פוסט", "תאריך קליטה", "סוג פוסט", "כמות לייקים", "כמות תגובות", "תמונה"]

class MediaRow(AsTableRow):
    def __init__(self, user_name: str, taken_at_ts: datetime, scrape_ts: datetime, media_type: str, likes_amount: int,
                 comments_amount: int, pic: HTMLImage):
        self.user_name = user_name
        self.taken_at_ts = taken_at_ts
        self.scrape_ts = scrape_ts
        if media_type == MediaTypes.PICTURE:
            self.media_type = "תמונה"
        elif media_type == MediaTypes.ALBUM:
            self.media_type = "אלבום"
        elif media_type == MediaTypes.VIDEO:
            self.media_type = "סרטון"
        else:
            self.media_type = "אחר"
        self.likes_amount = likes_amount
        self.comments_amount = comments_amount
        self.pic = pic

    @classmethod
    def get_export_fields(cls) -> List[str]:
        return ["user_name", "taken_at_ts", "scrape_ts", "media_type", "likes_amount", "comments_amount", "pic"]

    @classmethod
    def from_media_record(cls, media: MediaRecord):
        return MediaRow(media.owner_user_name, media.taken_at_ts, media.scrape_ts, media.media_type, media.likes_amount,
                        media.comments_amount, media.display_url)


class NewMediaReport(Report):
    def __init__(self, log_path: Optional[str] = None, log_level: Union[str, int] = logging.DEBUG,
                 log_to_console: bool = True):
        super().__init__(log_path, log_level, log_to_console)

    def get_new_media(self, mysql: MySQLHelper, from_date: Optional[datetime], to_date: Optional[datetime],
                      days_back: Optional[int]):
        assert from_date is not None or to_date is not None or days_back is not None
        ts_filter, ts_params = self.build_ts_filter(from_date, to_date, days_back, ts_col="taken_at_ts")
        query = """
        select *
        from media
        where {ts_filter}
        order by scrape_ts desc, taken_at_ts asc
        """.format(ts_filter=ts_filter)
        records = mysql.query(query, ts_params)
        media_records = [MediaRecord.from_row(record) for record in records]
        return media_records

    def prepare_mail(self, media: List[MediaRecord]):
        rows = ["<h3>גללתי ומצאתי!!</h3>"]
        if len(media) > 0:
            mutual_follow_event_rows = [MediaRow.from_media_record(x) for x in media]
            follow_table = HTMLTableConverter.to_table(mutual_follow_event_rows, headers=NEW_MEDIA_TABLE_HEADERS)
            rows.append("<h5>פוסטים חדשים:</h5>")
            rows.append(follow_table)

        msg = """
                <div dir="rtl">
                {0}
                </div>
                """.format('<br />'.join(rows))
        return msg

    def main(self, from_date: Optional[str] = None, to_date: Optional[str] = None, days_back: Optional[int] = None):
        assert from_date is not None or to_date is not None or days_back is not None
        if to_date is not None:
            to_date = datetime.strptime(to_date, DATE_FORMAT)
        if from_date is not None:
            from_date = datetime.strptime(from_date, DATE_FORMAT)
        mysql = MySQLHelper('mysql-insta-local')

        new_media = self.get_new_media(mysql, from_date, to_date, days_back)
        self.logger.info("%d new media objects were found", len(new_media))
        if len(new_media) >0:
            msg = self.prepare_mail(new_media)

            self.logger.info("Exporting email")
            exporter = SendGridExporter()
            resp = exporter.send_email('sidfeiner@gmail.com', ['sidfeiner@gmail.com'], 'גללתי ומצאתי - פוסטים חדשים',
                                       html_content=msg)

            self.logger.info("Done exporting.")
        else:
            self.logger.info("No data to send.")


if __name__ == '__main__':
    fire.Fire(NewMediaReport)
