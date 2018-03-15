import sqlite3
from memoized_property import memoized_property

from jardin.database.base import BaseConnection


class DatabaseConnection(BaseConnection):

    DRIVER = sqlite3

    @memoized_property
    def connect_args(self):
        return [self.db_config.path[1:]]

    @memoized_property
    def connect_kwargs(self):
        return {}

    @staticmethod
    def table_schema_query(table_name):
        return "pragma table_info(%s);" % table_name

    @staticmethod
    def transaction_begin_query():
        return 'BEGIN TRANSACTION;'
