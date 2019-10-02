from typing import List, Optional
import pyodbc
from pyodbc import Connection, Cursor, Row

from InstaProfiler.common.LoggerManager import LoggerManager


class Insertable(object):
    @classmethod
    def export_order(cls) -> List[str]:
        """return ordered list of fields to export"""
        raise NotImplemented()

    @classmethod
    def target_columns(cls) -> List[str]:
        """return list of target columns to insert the data into. Default is same as export_order"""
        return cls.export_order()


class Updatable(object):
    @classmethod
    def update_key(cls) -> List[str]:
        """return list of fields to update by"""
        raise NotImplemented()

    @classmethod
    def update_sql(cls) -> str:
        """sql to update fields"""
        raise NotImplemented()

    def update_params(self) -> Optional[List]:
        return None


class InsertableDuplicate(Insertable):
    @classmethod
    def on_duplicate_update_sql(cls) -> str:
        """Return query and params"""
        raise NotImplemented()

    def on_duplicate_update_params(self) -> List:
        """Returns the list of params to be used with on_duplicate_update_sql"""
        raise NotImplemented()

    def on_duplicate_update(self) -> (str, List):
        return self.on_duplicate_update_sql(), self.on_duplicate_update_params()


class MySQLHelper(object):
    def __init__(self, dsn_name: str):
        self.logger = LoggerManager.get_logger(__name__)
        self.connection = pyodbc.connect('DSN={0}'.format(dsn_name))  # type: Connection

    def get_cursor(self) -> Cursor:
        self.logger.info("Getting cursor...")
        return self.connection.cursor()  # type: Cursor

    def insert_ignore(self, table: str, records: List[Insertable], cursor: Optional[Cursor] = None):
        assert len(records[0].target_columns()) == len(records[0].export_order())
        self.logger.info("Inserting ignore...")
        columns = records[0].target_columns()
        sql = "INSERT IGNORE INTO {table} ({cols}) VALUES({vals})".format(table=table, cols=', '.join(columns),
                                                                          vals=', '.join(['?'] * len(columns)))
        data = [[getattr(record, field) for field in record.export_order()] for record in records]
        final_cursor = cursor or self.get_cursor()
        self.logger.debug("running sql: %s", sql)
        final_cursor.executemany(sql, data)
        if cursor is None:
            # Cursor was not given
            final_cursor.close()
        self.logger.info("Done inserting %d records", len(data))

    def insert_on_duplicate_update(self, table: str, records: List[InsertableDuplicate],
                                   cursor: Optional[Cursor] = None):
        assert len(records[0].target_columns()) == len(records[0].export_order())
        self.logger.info("Inserting (%d) on duplicate records...", len(records))
        columns = records[0].target_columns()
        update_sql = records[0].on_duplicate_update_sql()
        sql = "INSERT INTO {table} ({cols}) VALUES({vals}) ON DUPLICATE KEY UPDATE {update}".format(table=table,
                                                                                                    cols=', '.join(
                                                                                                        columns),
                                                                                                    vals=', '.join(
                                                                                                        ['?'] * len(
                                                                                                            columns)),
                                                                                                    update=update_sql)
        data = [[getattr(record, field) for field in record.export_order()] + record.on_duplicate_update_params() for
                record in records]
        final_cursor = cursor or self.get_cursor()
        self.logger.debug("running sql: %s", sql)
        final_cursor.executemany(sql, data)
        if cursor is None:
            final_cursor.close()
        self.logger.info("Done inserting %d records", len(data))

    def query(self, query: str, params: Optional[List] = None, cursor: Optional[Cursor] = None) -> List[Row]:
        final_cursor = cursor or self.get_cursor()
        self.logger.debug("Applying params: %s, to query %s", params, query)
        final_cursor.execute(query, params) if params is not None else cursor.execute(query)
        res = final_cursor.fetchall()
        if cursor is None:
            final_cursor.close()
        return res

    def execute(self, query: str, params: Optional[List] = None, cursor: Optional[Cursor] = None):
        final_cursor = cursor or self.get_cursor()
        self.logger.debug("Update. Applying params: %s, to query %s", params, query)
        res = final_cursor.execute(query, params) if params is not None else cursor.execute(query)
        if cursor is None:
            final_cursor.close()
        return res

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()
