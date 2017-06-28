from database import DatabaseAdapter, DatabaseConnections
from record import Record
import pandas as pd
import numpy as np
import re, inspect

class Model(pd.DataFrame):
  table_name = None
  table_alias = None
  db_names = {}
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
        return h.select(where = {h.belongs_to[this_table_name]: self.id})
      setattr(self, other_table_name, func)

  @classmethod
  def instance(self, result):
    return self.from_records(result[0], columns = result[1], coerce_float = True)

  @classmethod
  def stack_mark(self, stack):
    filename = stack[1][1]
    function_name = stack[1][3]
    line_number = stack[1][2]
    return ':'.join([filename, function_name, str(line_number)])

  @classmethod
  def select(self, **kwargs):
    kwargs['stack'] = self.stack_mark(inspect.stack())
    results = self.db_adapter(db_name = kwargs.get('db')).select(**kwargs)
    if kwargs.get('raw', False):
      return results
    else:
      return self.instance(results)

  @classmethod
  def count(self, **kwargs):
    kwargs['select'] = 'COUNT(*)'
    return self.db_adapter().select(**kwargs)[0][0]['count']

  def _instance_count(self, **kwargs): return super(Model, self).count(**kwargs)

  @classmethod
  def insert(self, **kwargs):
    return self.instance(self.db_adapter(rw = 'write').insert(**kwargs))

  def _instance_insert(self, *args, **kwargs): return super(Model, self).insert(*args, **kwargs)

  @classmethod
  def update(self, **kwargs):
    return self.instance(self.db_adapter(rw = 'write').update(**kwargs))

  def _instance_update(self, *args, **kwargs): return super(Model, self).update(*args, **kwargs)

  @classmethod
  def delete(self, **kwargs):
    return self.db_adapter(rw = 'write').delete(**kwargs)

  @classmethod
  def last(self, limit = 1):
    return self.instance(self.db_adapter().select(where = 'created_at IS NOT NULL', order = 'created_at DESC', limit = limit))

  @classmethod
  def find(self, id):
    return self.record_class(**self.db_adapter().select(where = {'id': id}, limit = 1)[0][0])

  @classmethod
  def db_adapter(self, rw = 'read', db_name = None):
    if not hasattr(self, '_db_adapter'): self._db_adapter = {}
    if rw not in self._db_adapter:
      self._db_adapter[rw] = DatabaseAdapter(self.db(rw = rw, db_name = db_name), self.model_metadata())
    return self._db_adapter[rw]

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
  def db(self, rw = 'read', db_name = None):
    if not hasattr(self, '_db'): self._db = {}
    name = self.db_names[rw] if db_name is None else db_name
    if name not in self._db: self._db[name] = DatabaseConnections.connection(name)
    return self._db[name]

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