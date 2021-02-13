import psycopg2 as pg
from psycopg2 import extras

from jardin.database.base_client import BaseClient
from jardin.database.base_lexicon import BaseLexicon
import jardin.config as config


class Lexicon(BaseLexicon):

    @staticmethod
    def table_schema_query(table_name):
        return "SELECT column_name, column_default, data_type FROM " \
            "information_schema.columns WHERE " \
            "table_name=%(table_name)s AND table_schema='public';"

    @staticmethod
    def column_info(row):
        return row['column_name'], row['column_default'], row['data_type']

    @staticmethod
    def update_values(fields, value_extrapolators):
        if len(fields) == 1:
            result = ', '.join(fields) + ' = ' + \
                [', '.join(ext)
                 for ext in value_extrapolators][0]
        else:
            result = '(' \
                + ', '.join(fields) \
                + ') = '
            result += ', '.join(
                ['(' + ', '.join(ext) + ')' for ext in value_extrapolators]
            )
        return result

    @staticmethod
    def row_ids(cursor, primary_key):
        row_ids = cursor.fetchall()
        return [r[primary_key] for r in row_ids]


class DatabaseClient(BaseClient):

    lexicon = Lexicon
    retryable_exceptions = (pg.OperationalError, pg.InterfaceError, pg.extensions.QueryCanceledError)

    def connect_impl(self):
        kwargs = self.default_connect_kwargs.copy()
        kwargs.update(connection_factory=extras.MinTimeLoggingConnection)
        conn = pg.connect(**kwargs)
        conn.initialize(config.logger)
        conn.autocommit = True
        return conn

    def execute_impl(self, conn, *query):
        cursor = conn.cursor(cursor_factory=pg.extras.RealDictCursor)
        cursor.execute(*query)
        return cursor
