import sqlite3

from jardin.database.base_client import BaseClient
from jardin.database.base_lexicon import BaseLexicon


class Lexicon(BaseLexicon):

    @staticmethod
    def table_schema_query(table_name):
        return "pragma table_info(%s);" % table_name

    @staticmethod
    def column_info(row):
        return row['name'], row['dflt_value'], row['type']

    @staticmethod
    def extrapolator(field):
        return ':%s' % field

    @staticmethod
    def row_ids(cursor, primary_key):
        return [cursor.lastrowid]


class DatabaseClient(BaseClient):

    lexicon = Lexicon
    retryable_exceptions = tuple()

    def connect_impl(self):
        # autocommit is enabled by setting isolation_level to None
        return sqlite3.connect(self.db_config.database, isolation_level=None)

    def execute_impl(self, conn, *query):
        cursor = conn.cursor()
        cursor.execute(*query)
        return cursor
