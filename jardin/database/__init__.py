from future.standard_library import install_aliases
install_aliases()
from urllib.parse import urlparse

import pandas

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

    SUPPORTED_SCHEMES = ('postgres', 'mysql', 'sqlite')

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
        elif db.scheme == 'postgres':
            import jardin.database.pg as driver
        elif db.scheme == 'mysql':
            import jardin.database.mysql as driver
        elif db.scheme == 'sqlite':
            import jardin.database.sqlite as driver

        return driver.DatabaseConnection(db, name)


    @classmethod
    def urls(self, name):
        if len(self._urls) == 0:
            config.init()
        for (nme, url) in config.DATABASES.items():
            if url:
                db = urlparse(url)
                if db.scheme == '':
                    db = urlparse('sqlite://localhost/%s' % url)
                self._urls[nme] = db
        return self._urls[name]


class DatabaseAdapter(object):

    def __init__(self, db, model_metadata):
        self.db = db
        self.model_metadata = model_metadata

    def select(self, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        kwargs['scheme'] = self.db.db_config.scheme
        query = SelectQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        results = self.db.cursor().fetchall()
        return pandas.DataFrame(list(results), columns=self.columns())

    def write(self, query_builder, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        kwargs['scheme'] = self.db.db_config.scheme
        query = query_builder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        row_ids = []
        if self.db.db_config.scheme == 'postgres':
            row_ids = self.db.cursor().fetchall()
            row_ids = [r[kwargs['primary_key']] for r in row_ids]
        if query_builder == InsertQueryBuilder:
            if self.db.db_config.scheme == 'mysql':
                config.logger.debug('SELECT LAST_INSERT_ID();')
                self.db.execute('SELECT LAST_INSERT_ID();')
                row_ids = [self.db.cursor().fetchall()[0][0]]
            if self.db.db_config.scheme == 'sqlite':
                row_ids = [self.db.cursor().lastrowid]
        if len(row_ids) > 0:
            return self.select(where={kwargs['primary_key']: row_ids})

        return pandas.DataFrame(columns=self.columns())

    def insert(self, **kwargs):
        return self.write(InsertQueryBuilder, **kwargs)

    def update(self, **kwargs):
        return self.write(UpdateQueryBuilder, **kwargs)

    def delete(self, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        kwargs['scheme'] = self.db.db_config.scheme
        query = DeleteQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)

    def raw_query(self, **kwargs):
        kwargs['scheme'] = self.db.db_config.scheme
        query = RawQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        if self.db.cursor().description:
            return self.db.cursor().fetchall(), self.columns()
        else:
            return None

    def columns(self):
        cursor_desc = self.db.cursor().description
        if cursor_desc:
            return [col_desc[0] for col_desc in cursor_desc]
        return []
