import pandas

from jardin.query_builders import \
    SelectQueryBuilder, \
    InsertQueryBuilder, \
    UpdateQueryBuilder, \
    DeleteQueryBuilder, \
    RawQueryBuilder
import jardin.config as config
from jardin.database.database_config import DatabaseConfig


class UnsupportedDriver(Exception): pass


class DatabaseConnections(object):

    _connections = {}
    _urls = {}

    SUPPORTED_SCHEMES = ('postgres', 'mysql', 'sqlite', 'snowflake', 'redshift')

    @classmethod
    def connection(self, db_name):
        if db_name not in self._connections:
            self._connections[db_name] = self.build_connection(db_name)
        return self._connections[db_name]

    @classmethod
    def build_connection(self, name):
        db = self.urls(name)
        if db.scheme not in self.SUPPORTED_SCHEMES:
            raise UnsupportedDriver('%s is not a supported driver' % db.scheme)
        elif db.scheme == 'postgres' or db.scheme == 'redshift':
            import jardin.database.drivers.pg as driver
        elif db.scheme == 'mysql':
            import jardin.database.drivers.mysql as driver
        elif db.scheme == 'sqlite':
            import jardin.database.drivers.sqlite as driver
        elif db.scheme == 'snowflake':
            import jardin.database.drivers.sf as driver

        return driver.DatabaseConnection(db, name)


    @classmethod
    def urls(self, name):
        if len(self._urls) == 0:
            config.init()
        for (nme, url) in config.DATABASES.items():
            if url:
                self._urls[nme] = DatabaseConfig(url)
        return self._urls[name]


def set_defaults(func):
    def wrapper(self, *args, **kwargs):
        kwargs.update(
            model_metadata=self.model_metadata,
            scheme=self.db.db_config.scheme,
            lexicon=self.db.lexicon
            )
        return func(self, *args, **kwargs)
    return wrapper


class DatabaseAdapter(object):

    def __init__(self, db, model_metadata):
        self.db = db
        self.model_metadata = model_metadata

    @set_defaults
    def select(self, **kwargs):
        query = SelectQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        results = self.db.cursor().fetchall()
        return pandas.DataFrame(list(results), columns=self.columns())

    @set_defaults
    def write(self, query_builder, **kwargs):
        query = query_builder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        row_ids = self.db.lexicon.row_ids(self.db, kwargs['primary_key'])
        if len(row_ids) > 0:
            return self.select(where={kwargs['primary_key']: row_ids})

        return pandas.DataFrame(columns=self.columns())

    def insert(self, **kwargs):
        return self.write(InsertQueryBuilder, **kwargs)

    def update(self, **kwargs):
        return self.write(UpdateQueryBuilder, **kwargs)

    @set_defaults
    def delete(self, **kwargs):
        query = DeleteQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)

    @set_defaults
    def raw_query(self, **kwargs):
        query = RawQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        if self.db.cursor().description:
            results = self.db.cursor().fetchall()
            return pandas.DataFrame(list(results), columns=self.columns())
        else:
            return None

    def columns(self):
        cursor_desc = self.db.cursor().description
        columns = []
        if cursor_desc:
            columns = [col_desc[0] for col_desc in cursor_desc]
            if self.db.db_config.lowercase_columns:
                columns = [col.lower() for col in columns]
        return columns
