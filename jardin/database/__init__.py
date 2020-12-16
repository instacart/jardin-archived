import threading

import pandas
import random
import re

from jardin.database.base import BaseClient
from jardin.query_builders import \
    SelectQueryBuilder, \
    InsertQueryBuilder, \
    UpdateQueryBuilder, \
    DeleteQueryBuilder, \
    RawQueryBuilder
import jardin.config as config
from jardin.database.database_config import DatabaseConfig


class UnsupportedDatabase(Exception): pass


class Datasources(object):

    class IsolatedDbClients(threading.local):
        def __init__(self) -> None:
            # All clients indexed by db_name.
            self.all = {}  # type: dict[str, list[BaseClient]]

            # Active clients indexed by db_name. An active client is a sticky connection for the
            # life of a session. The application can reset a session by calling `self.shuffle_clients()`
            # at an appropriate moment in the app lifecycle (e.g. the beginning of a job or request).
            self.active = {}  # type: dict[str, BaseClient]

    # Each thread will have its own cache of active database clients/connections
    _clients = IsolatedDbClients()

    # Mapping from db_name to a list of config info. The list will typically contain one element,
    # but in the case of a DB with many replicas, there will be one config object for each replica.
    # Read-only and shared across threads.
    _db_configs = {}  # type: dict[str, list[DatabaseConfig]]

    SUPPORTED_SCHEMES = ('postgres', 'mysql', 'sqlite', 'snowflake', 'redshift')

    @classmethod
    def active_client(self, db_name):
        if db_name not in self._clients.active:
            clients = self._clients.all.get(db_name)
            if clients is None:
                clients = self._build_clients(db_name)
                self._clients.all[db_name] = clients
            c = clients[0] if len(clients) == 1 else random.choice(clients)
            self.log_datasource(db_name, c.db_config)
            self._clients.active[db_name] = c
        return self._clients.active[db_name]

    @classmethod
    def _build_clients(self, name):
        clients = []
        configs = self.db_configs(name)
        for db in configs:
            if db.scheme not in self.SUPPORTED_SCHEMES:
                raise UnsupportedDatabase('%s is not a supported database' % db.scheme)
            elif db.scheme == 'postgres' or db.scheme == 'redshift':
                import jardin.database.clients.pg as impl
            elif db.scheme == 'mysql':
                import jardin.database.clients.mysql as impl
            elif db.scheme == 'sqlite':
                import jardin.database.clients.sqlite as impl
            elif db.scheme == 'snowflake':
                import jardin.database.clients.sf as impl
            clients.append(impl.DatabaseClient(db, name))
        return clients

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
    def shuffle_clients(self):
        for name, clients in self._clients.all.items():
            c = None
            if len(clients) == 1:
                c = clients[0]
            else:
                active = self._clients.active[name]
                filtered = list(filter(lambda x: x is not active, clients))
                c = filtered[0] if len(filtered) == 1 else random.choice(filtered)
            self.log_datasource(name, c.db_config)
            self._clients.active[name] = c

    @classmethod
    def log_datasource(self, name, db_config):
        host = getattr(db_config, 'host', None) or '_'  # use "_" for both missing attr or None value cases
        port = getattr(db_config, 'port', None) or '_'
        user = getattr(db_config, 'username', None) or '_'
        database = getattr(db_config, 'database', None) or '_'
        config.logger.debug("[{}]: datasource {}@{}:{}/{}".format(name, user, host, port, database))


def set_defaults(func):
    def wrapper(self, *args, **kwargs):
        kwargs.update(
            model_metadata=self.model_metadata,
            scheme=self.db_client.db_config.scheme,
            lexicon=self.db_client.lexicon
            )
        return func(self, *args, **kwargs)
    return wrapper


class DatabaseAdapter(object):

    def __init__(self, db_client, model_metadata):
        self.db_client = db_client
        self.model_metadata = model_metadata

    @set_defaults
    def select(self, **kwargs):
        query = SelectQueryBuilder(**kwargs).query
        config.logger.debug(query)
        results, columns = self.db_client.execute(*query, write=False)
        if results is None and columns is None:
            return None
        return pandas.DataFrame.from_records(results, columns=columns, coerce_float=True)

    @set_defaults
    def write(self, query_builder, **kwargs):
        query = query_builder(**kwargs).query
        config.logger.debug(query)
        returning_ids = self.db_client.execute(*query, write=True, **kwargs)
        if len(returning_ids) > 0:
            return self.select(where={kwargs['primary_key']: returning_ids})
        return None

    def insert(self, **kwargs):
        return self.write(InsertQueryBuilder, **kwargs)

    def update(self, **kwargs):
        return self.write(UpdateQueryBuilder, **kwargs)

    @set_defaults
    def delete(self, **kwargs):
        query = DeleteQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db_client.execute(*query, write=False)

    @set_defaults
    def raw_query(self, **kwargs):
        query = RawQueryBuilder(**kwargs).query
        config.logger.debug(query)
        results, columns = self.db_client.execute(*query, write=False)
        if results is None and columns is None:
            return None
        return pandas.DataFrame.from_records(results, columns=columns, coerce_float=True)