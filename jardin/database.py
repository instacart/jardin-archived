import psycopg2 as pg
from psycopg2 import extras
import urlparse
from query_builders import \
    SelectQueryBuilder, \
    InsertQueryBuilder, \
    UpdateQueryBuilder, \
    DeleteQueryBuilder, \
    RawQueryBuilder
import config
from tools import retry


class DatabaseConnections(object):

    _connections = {}
    _urls = {}

    @classmethod
    def connection(self, db_name):
        if db_name not in self._connections:
            self._connections[db_name] = self.build_connection(db_name)
        return self._connections[db_name]

    @classmethod
    def build_connection(self, name):
        db = self.urls(name)
        return DatabaseConnection(db)

    @classmethod
    def urls(self, name):
        if len(self._urls) == 0:
            config.init()
        for nme, url in config.DATABASES.iteritems():
            if url:
                self._urls[nme] = urlparse.urlparse(url)
        return self._urls[name]


class DatabaseConnection(object):

    _connection = None
    _cursor = None

    def __init__(self, db_config):
        self.db_config = db_config
        self.autocommit = True

    @retry(pg.OperationalError, tries=3)
    def connect(self):
        self._cursor = None
        connection = pg.connect(
            connection_factory=extras.MinTimeLoggingConnection,
            database=self.db_config.path[1:],
            user=self.db_config.username,
            password=self.db_config.password,
            host=self.db_config.hostname,
            port=self.db_config.port,
            connect_timeout=5)
        connection.initialize(config.logger)
        #connection.autocommit = True
        return connection

    def connection(self):
        if self._connection is None:
            self._connection = self.connect()
        return self._connection

    def cursor(self):
        if self._cursor is None:
            self._cursor = self.connection().cursor(
                cursor_factory=pg.extras.RealDictCursor)
        return self._cursor

    @retry(
        (pg.InterfaceError, pg.extensions.TransactionRollbackError),
        tries=3)
    def execute(self, *query):
        try:
            results = self.cursor().execute(*query)
            if self.autocommit:
                self.connection().commit()
            return results
        except pg.InterfaceError:
            self._connection = None
            self._cursor = None
            raise


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

    def insert(self, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        query = InsertQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        row_id = self.db.cursor().fetchone()['id']
        return self.select(where = {'id': row_id})

    def update(self, **kwargs):
        kwargs['model_metadata'] = self.model_metadata
        query = UpdateQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self.db.execute(*query)
        row_id = self.db.cursor().fetchone()['id']
        return self.select(where = {'id': row_id})

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
