import pymysql
from memoized_property import memoized_property

from jardin.tools import retry
from jardin.database.base import BaseClient
from jardin.database.lexicon import BaseLexicon


class Lexicon(BaseLexicon):

    @staticmethod
    def table_schema_query(table_name):
        return "SHOW COLUMNS FROM %s;" % table_name

    @staticmethod
    def column_info(row):
        return row['Field'], row['Default'], row['Type']

    @staticmethod
    def row_ids(cursor, primary_key):
        cursor.execute('SELECT LAST_INSERT_ID();')
        return [cursor.fetchall()[0][0]]

    @staticmethod
    def apply_watermark(query, watermark):
        return ' '.join([watermark, query])


class DatabaseClient(BaseClient):

    DRIVER = pymysql
    LEXICON = Lexicon

    @retry(pymysql.OperationalError, tries=3)
    def connect(self):
        return super().connect()

    @memoized_property
    def connect_kwargs(self):
        kwargs = super().connect_kwargs
        kwargs.update(autocommit=True)
        return kwargs

    @retry(pymysql.InterfaceError, tries=3)
    def execute(self, *query, write=False, **kwargs):
        return super().execute(*query, write=write, **kwargs)

