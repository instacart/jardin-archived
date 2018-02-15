from future.standard_library import install_aliases
install_aliases()
from urllib.parse import urlparse

from jardin.query_builders import \
    SelectQueryBuilder, \
    InsertQueryBuilder, \
    UpdateQueryBuilder, \
    DeleteQueryBuilder, \
    RawQueryBuilder
import jardin.config as config


class UnsupportedDriver(Exception): pass


class DatabaseConnections(object):

    _connections = {}
    _urls = {}

    ALLOWED_DRIVERS = ('postgres', 'mysql')

    @classmethod
    def connection(self, db_name):
        if db_name not in self._connections:
            self._connections[db_name] = self.build_connection(db_name)
        return self._connections[db_name]

    @classmethod
    def build_connection(self, name):
        db = self.urls(name)
        if db.scheme not in self.ALLOWED_DRIVERS:
            raise UnsupportedDriver('%s is not a supported driver' % db.scheme)
        elif db.scheme == 'postgres':
            import jardin.database.pg as driver
        elif db.scheme == 'mysql':
            import jardin.database.mysql as driver

        return driver.DatabaseConnection(db)


    @classmethod
    def urls(self, name):
        if len(self._urls) == 0:
            config.init()
        for (nme, url) in config.DATABASES.items():
            if url:
                self._urls[nme] = urlparse(url)
        return self._urls[name]


class DatabaseAdapter(object):

    def __init__(self, db, model_metadata):
        self.db = db
        self.model_metadata = model_metadata

    def select(self, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        query = SelectQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        return self.db.cursor().fetchall(), self.columns()

    def write(self, query_builder, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        query = query_builder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        row_ids = self.db.cursor().fetchall()
        row_ids = [r[kwargs['primary_key']] for r in row_ids]
        if len(row_ids) > 0:
            return self.select(where = {kwargs['primary_key']: row_ids})
        else:
            return ((), self.columns())

    def insert(self, **kwargs):
        return self.write(InsertQueryBuilder, **kwargs)

    def update(self, **kwargs):
        return self.write(UpdateQueryBuilder, **kwargs)

    def delete(self, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        query = DeleteQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)

    def raw_query(self, **kwargs):
        query = RawQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        if self.db.cursor().description:
            return self.db.cursor().fetchall(), self.columns()
        else:
            return None

    def columns(self):
        return [col_desc[0] for col_desc in self.db.cursor().description]
