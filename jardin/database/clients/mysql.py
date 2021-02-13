import pymysql

from jardin.database.base_client import BaseClient
from jardin.database.base_lexicon import BaseLexicon


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

    lexicon = Lexicon
    retryable_exceptions = (pymysql.InterfaceError, pymysql.OperationalError)

    def connect_impl(self):
        kwargs = self.default_connect_kwargs.copy()
        kwargs.update(autocommit=True)
        return pymysql.connect(**kwargs)

    def execute_impl(self, conn, *query):
        cursor = conn.cursor()
        cursor.execute(*query)
        return cursor
