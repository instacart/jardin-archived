import pandas
import random
import re

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

    _db_configs = {}          # For each db_name, it caches connection info of one or many DB instances

    _connections = {}         # For each db_name, it caches one or many connections

    _active_connections = {}  # For each db_name, it caches only ONE actively used connection. Active connection
                              # can only be changed by a call to self.shuffle_connections(). The idea is to provide
                              # connection stickiness during the lifetime of a session/task/job, etc.

    SUPPORTED_SCHEMES = ('postgres', 'mysql', 'sqlite', 'snowflake', 'redshift')

    @classmethod
    def connection(self, db_name):
        if db_name not in self._active_connections:
            connections = self._connections.get(db_name)
            if connections is None:
                connections = self.build_connections(db_name)
                self._connections[db_name] = connections
            c = connections[0] if len(connections) == 1 else random.choice(connections)
            config.logger.info("[{}]: database connection {}:{}".format(db_name, c.db_config.host, c.db_config.port))
            self._active_connections[db_name] = c
        return self._active_connections[db_name]

    @classmethod
    def build_connections(self, name):
        connections = []
        configs = self.db_configs(name)
        for db in configs:
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
            connections.append(driver.DatabaseConnection(db, name))
        return connections

    @classmethod
    def db_configs(self, name):
        if len(self._db_configs) == 0:
            config.init()
            for (nme, urls) in config.DATABASES.items():
                if not urls:
                    continue
                # we don't support multi-configs of dictionary format yet; the "else [urls]" is for a dictionary
                url_list = re.split(r'\s+', urls) if isinstance(urls, str) else [urls]
                self._db_configs[nme] = list(map(lambda x: DatabaseConfig(x), url_list))
        return self._db_configs[name]

    @classmethod
    def shuffle_connections(self):
        for name, conns in self._connections.items():
            c = None
            if len(conns) == 1:
                c = conns[0]
            else:
                active = self._active_connections[name]
                filtered = list(filter(lambda x: x is not active, conns))
                c = filtered[0] if len(filtered) == 1 else random.choice(filtered)
            config.logger.info("[{}]: database connection {}:{}".format(name, c.db_config.host, c.db_config.port))
            self._active_connections[name] = c


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
        return pandas.DataFrame.from_records(list(results), columns=self.columns(), coerce_float=True)

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
            return pandas.DataFrame.from_records(list(results), columns=self.columns(), coerce_float=True)
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
