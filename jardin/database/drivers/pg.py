import sys
import psycopg2 as pg
from psycopg2.pool import SimpleConnectionPool, ThreadedConnectionPool
from psycopg2 import extras
from memoized_property import memoized_property

from jardin.tools import retry
from jardin.database.base import BaseConnection
from jardin.database.lexicon import BaseLexicon
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


class DatabaseConnection(BaseConnection):

    DRIVER = pg
    LEXICON = Lexicon

    @retry(pg.OperationalError, tries=3)
    def get_connection(self):
        connection = super(DatabaseConnection, self).get_connection()
        connection.initialize(config.logger)
        return connection

    @memoized_property
    def connect_kwargs(self):
        kwargs = super(DatabaseConnection, self).connect_kwargs
        kwargs.update(connection_factory=extras.MinTimeLoggingConnection)
        return kwargs

    @memoized_property
    def cursor_kwargs(self):
        return dict(cursor_factory=pg.extras.RealDictCursor)

    @memoized_property
    def pool(self):
        if self.pool_config is None:
            return None
        pool_name = self.pool_config.get("pool", None)
        if pool_name is None:
            return None
        pool = getattr(sys.modules[__name__], pool_name)
        min_connections = self.pool_config.get("min_connections", 2)
        max_connections = self.pool_config.get("max_connections", min_connections * 2)
        return pool(min_connections, max_connections, **self.connect_kwargs)

    @retry((pg.InterfaceError, pg.extensions.TransactionRollbackError, pg.extensions.QueryCanceledError), tries=3)
    def execute(self, *query, write=False, **kwargs):
        return super(DatabaseConnection, self).execute(*query, write=write, **kwargs)
