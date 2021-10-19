import random
import re
import threading

from jardin import config as config
from jardin.database.database_config import DatabaseConfig


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

    # Guards lazy initializers
    _lock = threading.Lock()

    SUPPORTED_SCHEMES = ('postgres', 'mysql', 'sqlite',
                         'snowflake', 'redshift')

    @classmethod
    def db_config(self, db_name):
        return self.db_configs(db_name)[0]

    @classmethod
    def db_lexicon(self, db_name):
        # The lexicon is a helper object, so we can fetch it without needing an actual
        # db connection, so any client would work, even banned ones
        self._populate_client_lists_if_needed(db_name)
        any_client = self._clients.all[db_name][0]
        return any_client.__class__.lexicon

    @classmethod
    def _populate_client_lists_if_needed(self, db_name):
        if self._clients.all.get(db_name) is None:
            self._clients.all[db_name] = self._build_clients(db_name)

    @classmethod
    def client_generator(self, db_name):
        self._populate_client_lists_if_needed(db_name)

        while True:  # yield connections forever
            memoized_client = self._clients.active.get(db_name, None)
            if memoized_client is None or memoized_client.is_banned:
                potential_clients = self.non_banned_clients(db_name)
                if len(potential_clients) == 0:
                    config.logger.error("[{}] Client Generator yielded no connections".format(db_name))
                    self._clients.active[db_name] = None
                else:
                    self._clients.active[db_name] = random.choice(potential_clients)
            yield self._clients.active[db_name]

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
        if not self._db_configs:
            with self._lock:
                if not self._db_configs:
                    self._db_configs = self._build_db_configs()
        return self._db_configs[name]

    @classmethod
    def _build_db_configs(self):
        config.init()
        d = dict()
        for (db_name, val) in config.DATABASES.items():
            # we don't support multi-configs of dictionary format yet; the "else [urls]" is for a dictionary
            url_list = re.split(r'\s+', val.strip()) if isinstance(val, str) else [val]
            d[db_name] = [DatabaseConfig(x, db_name) for x in url_list]
        return d

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
    def non_banned_clients(self, name):
        return [client for client in self._clients.all.get(name, []) if not client.is_banned]

    @classmethod
    def unban_all_clients(self, name):
        clients = self._clients.all.get(name, [])
        for client in clients:
            client.unban()

    @classmethod
    def log_datasource(self, name, db_config):
        host = getattr(db_config, 'host', None) or '_'  # use "_" for both missing attr or None value cases
        port = getattr(db_config, 'port', None) or '_'
        user = getattr(db_config, 'username', None) or '_'
        database = getattr(db_config, 'database', None) or '_'
        config.logger.debug("[{}]: datasource {}@{}:{}/{}".format(name, user, host, port, database))


class UnsupportedDatabase(Exception): pass
