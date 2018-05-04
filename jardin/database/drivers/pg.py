import psycopg2 as pg
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
        result = '(' \
            + ', '.join(fields) \
            + ') = '
        result += ', '.join(
            ['(' + ', '.join(ext) + ')' for ext in value_extrapolators]
            )
        return result

    @staticmethod
    def row_ids(db, primary_key):
        row_ids = db.cursor().fetchall()
        row_ids = [r[primary_key] for r in row_ids]
        return row_ids


class DatabaseConnection(BaseConnection):

    DRIVER = pg
    LEXICON = Lexicon

    @retry(pg.OperationalError, tries=3)
    def connect(self):
        connection = super(DatabaseConnection, self).connect()
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

    @retry(
        (
            pg.InterfaceError,
            pg.extensions.TransactionRollbackError,
            pg.extensions.QueryCanceledError
            ),
        tries=3)
    def execute(self, *query):
        return super(DatabaseConnection, self).execute(*query)
