import psycopg2 as pg
from psycopg2 import extras
from memoized_property import memoized_property
import urlparse
from query_builders import SelectQueryBuilder, InsertQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder
import config
import pandas.io.sql as psql
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

  def __init__(self, db_config, autoconnect=False):
    self.db_config = db_config
    self._connection = None
    self._cursor = None
    if autoconnect:
      self.connect()

  def __str__(self):
    return "Connection[{hostname}]:{port} > {user}@{dbname}".format(
      hostname=self.db_config.hostname, port=self.db_config.port, user=self.db_config.username, dbname=self.db_config.path[1:])

  def __repr__(self):
    return self.__str__()

  @retry(pg.OperationalError, tries=3)
  def connect(self):
    if self._connection:
      return

    self._connection = pg.connect(
      connection_factory=extras.MinTimeLoggingConnection,
      database=self.db_config.path[1:],
      user=self.db_config.username,
      password=self.db_config.password,
      host=self.db_config.hostname,
      port=self.db_config.port,
      connect_timeout=5
    )
    self._connection.initialize(config.logger)
    self._connection.autocommit = True
    self._cursor = self._connection.cursor(cursor_factory=pg.extras.RealDictCursor)

  @retry((pg.InterfaceError, pg.extensions.TransactionRollbackError), tries=3)
  def execute(self, *query):
    self.connect()
    try:
      self._cursor.execute(*query)
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
    if kwargs.get('raw', False):
      return psql.read_sql(sql = query[0], params = query[1], con = self.db)
    else:
      self.db.execute(*query)
      return self.db._cursor.fetchall(), self.columns()

  def insert(self, **values):
    query = InsertQueryBuilder(values = values, model_metadata = self.model_metadata).query
    config.logger.debug(query)
    self.db.execute(*query)
    row_id = self.db._cursor.fetchone()['id']
    return self.select(where = {'id': row_id})

  def update(self, **kwargs):
    kwargs['model_metadata'] = self.model_metadata
    query = UpdateQueryBuilder(**kwargs).query
    config.logger.debug(query)
    self.db.execute(*query)
    row_id = self.db._cursor.fetchone()['id']
    return self.select(where = {'id': row_id})

  def delete(self, **kwargs):
    kwargs['model_metadata'] = self.model_metadata
    query = DeleteQueryBuilder(**kwargs).query
    config.logger.debug(query)
    self.db.execute(*query)

  def columns(self):
    return [col_desc[0] for col_desc in self.db._cursor.description]
