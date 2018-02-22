import psycopg2 as pg
from psycopg2 import extras
from memoized_property import memoized_property

from jardin.tools import retry
from jardin.database.base import BaseConnection
import jardin.config as config


class DatabaseConnection(BaseConnection):

    DRIVER = pg

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
