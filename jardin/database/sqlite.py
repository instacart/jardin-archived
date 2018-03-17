import sqlite3
from memoized_property import memoized_property

from jardin.database.base import BaseConnection
from jardin.database.lexicon import BaseLexicon


class Lexicon(BaseLexicon):

    @staticmethod
    def table_schema_query(table_name):
        return "pragma table_info(%s);" % table_name

    @staticmethod
    def transaction_begin_query():
        return 'BEGIN TRANSACTION;'

    @staticmethod
    def column_name_default(row):
        return row[1], row[4]

    @staticmethod
    def extrapolator(field):
        return ':%s' % field

    @staticmethod
    def row_ids(db, primary_key):
        return [db.cursor().lastrowid]


class DatabaseConnection(BaseConnection):

    DRIVER = sqlite3
    LEXICON = Lexicon

    @memoized_property
    def connect_args(self):
        return [self.db_config.path[1:]]

    @memoized_property
    def connect_kwargs(self):
        return {}
