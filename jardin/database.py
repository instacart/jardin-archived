import psycopg2 as pg
from psycopg2 import extras
from memoized_property import memoized_property
import urlparse
from query_builders import SelectQueryBuilder, InsertQueryBuilder, UpdateQueryBuilder
import config

class DatabaseConnections():
  
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
    connection = pg.connect(connection_factory = extras.MinTimeLoggingConnection, database = db.path[1:], user = db.username, password = db.password, host = db.hostname, port = db.port, connect_timeout = 5)
    connection.initialize(config.logger)
    connection.autocommit = True
    return connection

  @classmethod
  def urls(self, name):
    if len(self._urls) == 0:
      config.init()
      for nme, url in config.DATABASES.iteritems():
        self._urls[nme] = urlparse.urlparse(url)
    return self._urls[name]

class DatabaseAdapter():

  def __init__(self, db, model_metadata):
    self.db = db
    self.model_metadata = model_metadata

  @memoized_property
  def cursor(self):
    return self.db.cursor(cursor_factory = pg.extras.RealDictCursor)

  def select(self, **kwargs):
    kwargs['model_metadata'] = self.model_metadata
    query = SelectQueryBuilder(**kwargs).query
    config.logger.debug(query)
    self.cursor.execute(*query)
    return self.cursor.fetchall(), self.columns()

  def insert(self, **values):
    query = InsertQueryBuilder(values = values, model_metadata = self.model_metadata).query
    self.cursor.execute(*query)
    row_id = self.cursor.fetchone()['id']
    return self.select(where = {'id': row_id})

  def update(self, **kwargs):
    kwargs['model_metadata'] = self.model_metadata
    query = UpdateQueryBuilder(**kwargs).query
    self.cursor.execute(*query)

  def columns(self):
    return [col_desc[0] for col_desc in self.cursor.description]
