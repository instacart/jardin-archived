from database import DatabaseAdapter, DatabaseConnections
from record import Record
import pandas as pd
import numpy as np
import re, inspect
import os


class Model(pd.DataFrame):
  """
    Base class from which your models should inherit.
  """
  ROLES = {"replica": "read", "master": "write"}  # to have backward compatibility
  table_name = None
  table_alias = None
  db_names = {} # {'master': database_master_url, 'replica': database_replica_url}
  has_many = []
  belongs_to = {}
  scopes = {}
  record_class = Record

  def __init__(self, *args, **kwargs):
    self.create_relationships()
    self.count = self._instance_count
    self.insert = self._instance_insert
    self.update = self._instance_update
    super(Model, self).__init__(*args, **kwargs)

  @property
  def _constructor(self):
    return self.__class__

  @classmethod
  def create_relationships(self):
    this_table_name = self.model_metadata()['table_name']
    for h in self.has_many:
      other_table_name = h.model_metadata()['table_name']
      def func(self):
        return h.select(where={h.belongs_to[this_table_name]: self.id})
      setattr(self, other_table_name, func)

  @classmethod
  def instance(self, result):
    return self.from_records(result[0], columns=result[1], coerce_float=True)

  @classmethod
  def stack_mark(self, stack, db_conn=None):
    filename = stack[1][1]
    function_name = stack[1][3]
    line_number = stack[1][2]
    stack = [filename, function_name, str(line_number)]
    if db_conn:
      stack = [db_conn.connection().dsn] + stack
    return ':'.join(stack)

  @classmethod
  def select(self, **kwargs):
    #select='*', where=None, inner_joins=None, left_joins=None, 
    #group=None, order=None, limit=None, db=None, role='replica'):
    """
    Perform a SELECT statement on the model's table in the replica database.

    :param select: Columns to return in the SELECT statement.
    :type select: string, array
    :param where: WHERE clause of the SELECT statement. This can be a plain string, a dict or an array.
    :type where: string, dict, array
    :param inner_joins: Specifies an INNER JOIN clause. Can be a plain string (without the INNER JOIN part), an array of strings or an array of classes if the relationship was defined in the model.
    :type inner_joins: string, array
    :param left_joins: Specifies an LEFT JOIN clause. Can be a plain string (without the LEFT JOIN part), an array of strings or an array of classes if the relationship was defined in the model.
    :type left_joins: string, array
    :param group: Specifies a GROUP BY clause.
    :type group: string
    :param order: ORDER BY clause.
    :type order: string
    :param limit: LIMIT clause.
    :type limit: integer
    :param db: Database name from your ``jardin_conf.py``, overrides the default database set in the model declaration.
    :type db: string
    :param role: One of ``('master', 'replica')`` to override the default.
    :type role: string
    :returns: ``jardin.Model`` instance, which is a ``pandas.DataFrame``.
    """
    db_adapter = self.db_adapter(db_name=kwargs.get('db'), role=kwargs.get('role', 'replica'))
    kwargs['stack'] = self.stack_mark(inspect.stack(), db_conn=db_adapter.db)
    return self.instance(db_adapter.select(**kwargs))

  @classmethod
  def query(self, sql=None, filename=None, **kwargs):
    """ run raw sql from sql or file against.

    :param sql: Raw SQL query to pass directly to the connection.
    :type sql: string
    :param filename: Path to a file containing a SQL query. The path should be relative to CWD.
    :type filename: string
    :param db: `optional` Database name from your ``jardin_conf.py``, overrides the default database set in the model declaration.
    :type db: string
    :param role: `optional` One of ``('master', 'replica')`` to override the default.
    :type role: string
    :returns: ``jardin.Model`` instance, which is a ``pandas.DataFrame``.
    """
    kwargs['stack'] = self.stack_mark(inspect.stack())
    if filename:
      filename = os.path.join(os.environ['PWD'], filename)
    kwargs['where'] = kwargs.get('where', kwargs.get('params'))
    results = self.db_adapter(db_name=kwargs.get('db'), role=kwargs.get('role', 'replica')).raw_query(sql=sql, filename=filename, **kwargs)
    if results:
      return self.instance(results)
    else:
      return None


  @classmethod
  def count(self, **kwargs):
    """
    Performs a COUNT statement on the model's table in the replica database.

    :param where: WHERE clause of the SELECT statement. This can be a plain string, a dict or an array.
    :type where: string, dict, array
    :param db: Database name from your ``jardin_conf.py``, overrides the default database set in the model declaration.
    :type db: string
    :param role: One of ``('master', 'replica')`` to override the default.
    :type role: string
    :returns: integer
    """
    kwargs['select'] = 'COUNT(*)'
    return self.db_adapter(db_name=kwargs.get('db'), role=kwargs.get('role', 'replica')).select(**kwargs)[0][0]['count']

  def _instance_count(self, **kwargs): return super(Model, self).count(**kwargs)

  @classmethod
  def insert(self, **kwargs):
    """
    Performs an INSERT statement on the model's table in the master database.

    :param values: A dictionary containing the values to be inserted. ``datetime``, ``dict`` and ``bool`` objects can be passed as is and will be correctly serialized by psycopg2.
    :type values: dict
    """
    kwargs['stack'] = self.stack_mark(inspect.stack())
    return self.record_class(**self.db_adapter(role='master').insert(**kwargs)[0][0])

  def _instance_insert(self, *args, **kwargs): return super(Model, self).insert(*args, **kwargs)

  @classmethod
  def update(self, **kwargs):
    """
    Performs an UPDATE statement on the model's table in the master database.

    :param values: A dictionary of values to update. ``datetime``, ``dict`` and ``bool`` objects can be passed as is and will be correctly serialized by psycopg2.
    :type values: dict
    :param where: The WHERE clause. This can be a plain string, a dict or an array.
    :type where: string, dict, array
    """
    kwargs['stack'] = self.stack_mark(inspect.stack())
    return self.instance(self.db_adapter(role='master').update(**kwargs))

  def _instance_update(self, *args, **kwargs): return super(Model, self).update(*args, **kwargs)

  @classmethod
  def delete(self, **kwargs):
    """
    Performs a DELETE statement on the model's table in the master database.

    :param where: The WHERE clause. This can be a plain string, a dict or an array.
    :type where: string, dict, array
    """
    kwargs['stack'] = self.stack_mark(inspect.stack())
    return self.db_adapter(role='master').delete(**kwargs)

  @classmethod
  def last(self, limit=1, **kwargs):
    """
    Returns the last `limit` records inserted in the model's table in the replica database. Rows are sorted by ``created_at``.
    """
    return self.instance(self.db_adapter(db_name=kwargs.get('db'), role=kwargs.get('role', 'replica')).select(where='created_at IS NOT NULL', order='created_at DESC', limit=limit))

  @classmethod
  def find_by(self, values={}, **kwargs):
    """
    Returns a single record matching the criteria in ``values`` found in the model's table in the replica database.

    :param values: Criteria to find the record.
    :type values: dict
    :returns: an instance of the model's record class, i.e. :doc:`jardin_record` by default.
    """
    try:
      return self.record_class(**self.db_adapter(
        db_name=kwargs.get('db'),
        role=kwargs.get('role', 'replica')).select(where=values, limit=1)[0][0])
    except IndexError:
      return None

  @classmethod
  def find(self, id, **kwargs):
    """
    Finds a record by its id in the model's table in the replica database.
    :returns: an instance of the model's record class, i.e. :doc:`jardin_record` by default.
    """
    return self.find_by(values={'id': id}, **kwargs)

  @classmethod
  def db_adapter(self, role='replica', db_name=None):
    if not hasattr(self, '_db_adapter'): self._db_adapter = {}
    if role not in self._db_adapter:
      self._db_adapter[role] = DatabaseAdapter(self.db(role=role, db_name=db_name), self.model_metadata())
    return self._db_adapter[role]

  @classmethod
  def model_metadata(self):
    tn = self.table_name if isinstance(self.table_name, str) else self.default_table_name()
    table_alias = self.table_alias
    if table_alias is None: table_alias = ''.join([w[0] for w in tn.split('_')])
    return {
      'table_name': tn,
      'table_alias': table_alias,
      'belongs_to': self.belongs_to,
      'scopes': self.scopes
    }

  @classmethod
  def default_table_name(self):
    if '_default_table_name' not in self.__dict__:
      name = self.__name__
      s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
      self._default_table_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return self._default_table_name

  @classmethod
  def db(self, role='replica', db_name = None):
    if not hasattr(self, '_db'): self._db = {}
    name = self.db_names.get(role, self.db_names.get(self.ROLES.get(role))) if db_name is None else db_name
    if name not in self._db: self._db[name] = DatabaseConnections.connection(name)
    return self._db[name]

  @classmethod
  def _use_replica(self, **kwargs):
    try:
      kwargs['stack'] = self.stack_mark(inspect.stack())
      sql = "select setting FROM pg_settings WHERE name = 'hot_standby'"
      r = self.instance(self.db_adapter().raw_query(sql=sql, **kwargs)).squeeze()
      return r == "on"
    except:
      return False

  @classmethod
  def replica_lag(self, **kwargs):
    """
    Returns the current replication lag in seconds between the master and replica databases.

    :returns: float
    """
    if not self._use_replica():
      return 0
    try:
      kwargs['stack'] = self.stack_mark(inspect.stack())
      sql = "select EXTRACT(EPOCH FROM NOW() - pg_last_xact_replay_timestamp()) AS replication_lag"
      return self.instance(self.db_adapter().raw_query(sql=sql, **kwargs)).squeeze()
    except:
      return 0

  @classmethod
  def transaction(self):
    """
    Enables multiple statements to be ran within a single transaction, see :doc:`features`.
    """
    return Transaction(self)

  def where(self, **kwargs):
    conditions = kwargs.get('conditions', kwargs)
    if 'where_not' in conditions:
      wnot = conditions['where_not']
      del conditions['where_not']
    else:
      wnot = False
    filt = True
    for field, value in conditions.iteritems():
      if value is None:
        nf = pd.isnull(self[field])
      elif isinstance(value, (np.ndarray, list, pd.Series)):
        nf = self[field].isin(value)
      else:
        nf = self[field] == value
      if wnot: nf = ~nf
      filt = filt & nf
    return self[filt]

  def where_not(self, **kwargs):
    kwargs['where_not'] = True
    return self.where(**kwargs)

  def records(self):
    """
    Returns an iterator to loop over the rows, each being an instance of the model's record class, i.e. :doc:`jardin_record` by default.
    """
    return ModelIterator(self)

class ModelIterator(object):

  def __init__(self, model):
    self.model = model
    self.current = 0

  def __iter__(self): return self

  def next(self):
    if self.current < len(self.model):
      record = self.model.record_class(**self.model.iloc[self.current])
      self.current += 1
      return record
    else:
      raise StopIteration()


class Transaction(object):

  def __init__(self, model):
    self._model = model
    self._connection = self._model.db(role='master')

  def __enter__(self):
    self._connection.autocommit = False
    self._model.query(sql='BEGIN;', role='master')

  def __exit__(self, type, value, traceback):
    if value is None:
      self._model.query(sql='COMMIT;', role='master')
    else:
      self._model.query(sql='ROLLBACK;', role='master')
    self._connection.autocommit = True