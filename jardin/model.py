from datetime import datetime
import pandas
import re, inspect
import os

import config
from database import DatabaseAdapter, DatabaseConnections


class Collection(pandas.DataFrame):
    """
        Base class for collection of records. Inherits from `pandas.DataFrame`.
    """

    @property
    def _constructor(self):
        return self.__class__

    def records(self):
        """
        Returns an iterator to loop over the rows, each being an instance of the model's record class, i.e. :doc:`jardin_record` by default.
        """
        return ModelIterator(self)

    def index_by(self, field):
        """
        Returns a dict with a key for each value of `field` and the first record with that value as value.
        :param field: Name of the field to index by.
        :type field: string.
        """
        values = self[field].unique()
        results = {}
        for value in values:
            results[value] = self.model_class(**self[self[field] == value].iloc[0])
        return results


class Model(object):
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
    collection_class = Collection
    primary_key = 'id'

    def __init__(self, **kwargs):
        self.attributes = dict()
        columns = self.__class__.column_names()
        self.attributes[self.primary_key] = kwargs.get(self.primary_key, None)
        for column in set(columns + kwargs.keys()):
            self.attributes[column] = kwargs.get(column)
            if column in columns and column in kwargs:
                del kwargs[column]
        super(Model, self).__init__(**kwargs)

    def __getattribute__(self, i):
        try:
            return super(Model, self).__getattribute__(i)
        except AttributeError as e:
            if i in self.attributes:
                return self.attributes[i]
            raise e

    def __setattr__(self, i , v):
        if i == 'attributes':
            super(Model, self).__setattr__(i, v)
        else:
            self.attributes[i] = v
            return v

    def __repr__(self):
        attrs = []
        if 'id' in self.attributes:
            attrs += ['id=%s' % self.id]
        for att_name, attr_value in self.attributes.iteritems():
            if att_name == 'id': continue
            attrs += ['%s=%s' % (att_name, attr_value.__repr__())]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(attrs))

    def __len__(self):
        non_null = 0
        for v in self.attributes.values():
            if v is not None: non_null += 1
        return non_null

    def __getitem__(self, key):
        return self.attributes[key]

    def __setitem__(self, key, value):
        self.attributes[key] = value

    def __delitem__(self, key):
        del self.attributes[key]

    @classmethod
    def _collection_class(self):
        class _Collection(self.collection_class): pass
        _Collection.model_class = self
        return _Collection

    @classmethod
    def collection(self, **kwargs):
        return self._collection_class()(**kwargs)

    def save(self):
        if self.attributes.get('id'):
            return self.__class__.update(values=self, where={'id': self.id})
        else:
            return self.__class__.insert(values=self)

    #def init_relationships(self):
    #    for h in self.has_many:
    #        def func(self, select=None, where=None, limit=None, db=None, role=None):

    #@classmethod
    #def create_relationships(self):
    #    this_table_name = self.model_metadata()['table_name']
    #    for h in self.has_many:
    #        other_table_name = h.model_metadata()['table_name']
    #        def func(self):
    #          return h.select(where={h.belongs_to[this_table_name]: self.id})
    #        setattr(self, other_table_name, func)

    @classmethod
    def instance(self, result):
        return self._collection_class().from_records(
            result[0],
            columns=result[1],
            coerce_float=True
            )

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
        :type select: string, array, dict
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
        :returns: ``jardin.Collection`` instance, which is a ``pandas.DataFrame``.
        """
        db_adapter = self.db_adapter(
            db_name=kwargs.get('db'),
            role=kwargs.get('role') or 'replica'
            )

        kwargs['stack'] = self.stack_mark(
            inspect.stack(),
            db_conn=db_adapter.db
            )

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
        :returns: ``jardin.Collection`` instance, which is a ``pandas.DataFrame``.
        """
        kwargs['stack'] = self.stack_mark(inspect.stack())
        
        if filename:
            filename = os.path.join(os.environ['PWD'], filename)
        
        kwargs['where'] = kwargs.get('where', kwargs.get('params'))
        
        results = self.db_adapter(
            db_name=kwargs.get('db'),
            role=kwargs.get('role') or 'replica'
            ).raw_query(
                sql=sql,
                filename=filename,
                **kwargs
                )

        if results:
            return self.instance(results)
        else:
            return None


    @classmethod
    def count(self, **kwargs):
        """
        Performs a COUNT statement on the model's table in the replica database.

        :param select: Column to be counted.
        :type select: string
        :param where: WHERE clause of the SELECT statement. This can be a plain string, a dict or an array.
        :type where: string, dict, array
        :param db: Database name from your ``jardin_conf.py``, overrides the default database set in the model declaration.
        :type db: string
        :param role: One of ``('master', 'replica')`` to override the default.
        :type role: string
        :returns: integer
        """
        if 'select' in kwargs:
            kwargs['select'] = 'COUNT(%s)' % kwargs['select']
        else:
            kwargs['select'] = 'COUNT(*)'
        return self.db_adapter(
            db_name=kwargs.get('db'),
            role=kwargs.get('role') or 'replica'
            ).select(**kwargs)[0][0]['count']

    @classmethod
    def insert(self, **kwargs):
        """
        Performs an INSERT statement on the model's table in the master database.

        :param values: A dictionary containing the values to be inserted. ``datetime``, ``dict`` and ``bool`` objects can be passed as is and will be correctly serialized by psycopg2.
        :type values: dict
        """
        if len(kwargs['values']) == 0:
            config.logger.warning('No values to insert.')
            return
        values = kwargs['values']
        if isinstance(values, self):
            values = values.attributes.copy()
        if isinstance(values, dict):
            for k, v in values.iteritems():
                if v is None:
                    del kwargs['values'][k]
        kwargs['stack'] = self.stack_mark(inspect.stack())
        kwargs['primary_key'] = self.primary_key
        column_names = self.column_names()
        now = datetime.utcnow()
        for field in ('created_at', 'updated_at'):
            if field in column_names:
                kwargs['values'][field] = now
        results = self.db_adapter(role='master').insert(**kwargs)
        return self.record_or_model(results)

    @classmethod
    def record_or_model(self, results):
        if len(results[0]) == 1:
            return self(**results[0][0])
        else:
            return self.instance(results)

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
        kwargs['primary_key'] = self.primary_key
        column_names = self.column_names()
        now = datetime.utcnow()
        if 'updated_at' in column_names:
            kwargs['values']['updated_at'] = now
        results = self.db_adapter(role='master').update(**kwargs)
        return self.record_or_model(results)

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
        return self.instance(
            self.db_adapter(
                db_name=kwargs.get('db'),
                role=kwargs.get('role') or 'replica'
                ).select(
                    where='created_at IS NOT NULL',
                    order='created_at DESC',
                    limit=limit
                    )
                )

    @classmethod
    def find_by(self, values={}, **kwargs):
        """
        Returns a single record matching the criteria in ``values`` found in the model's table in the replica database.

        :param values: Criteria to find the record.
        :type values: dict
        :returns: an instance of the model.
        """
        try:
            return self(**self.db_adapter(
                db_name=kwargs.get('db'),
                role=kwargs.get('role') or 'replica'
                ).select(
                    where=values,
                    limit=1
                    )[0][0]
                )
        except IndexError:
            return None

    @classmethod
    def find(self, id, **kwargs):
        """
        Finds a record by its id in the model's table in the replica database.
        :returns: an instance of the model.
        """
        return self.find_by(values={'id': id}, **kwargs)

    @classmethod
    def db_adapter(self, role='replica', db_name=None):
        if not hasattr(self, '_db_adapter'):
            self._db_adapter = {}
        
        if role not in self._db_adapter:
          self._db_adapter[role] = DatabaseAdapter(
              self.db(
                  role=role,
                  db_name=db_name
                  ), 
              self.model_metadata()
              )

        return self._db_adapter[role]

    @classmethod
    def model_metadata(self):
        table_name = self.table_name or self._default_table_name()
        table_alias = self.table_alias or self._default_table_alias(table_name)
        return {
            'table_name': table_name,
            'table_alias': table_alias,
            'belongs_to': self.belongs_to,
            'scopes': self.scopes
        }

    @staticmethod
    def _default_table_alias(table_name):
        return ''.join([w[0] for w in table_name.split('_')])

    @classmethod
    def _default_table_name(self):
        import inflect
        name = self.__name__
        #s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        s1 = re.sub('([A-Z])', r'_\1', name)[1:]
        s1 = s1.split('_')
        s1[-1] = inflect.engine().plural_noun(s1[-1])
        return '_'.join(map(lambda x: x.lower(), s1))

    @classmethod
    def db(self, role='replica', db_name = None):
        if not hasattr(self, '_db'): self._db = {}
        name = self.db_names.get(
            role,
            self.db_names.get(self.ROLES.get(role))) if db_name is None else db_name
        
        if name not in self._db:
            self._db[name] = DatabaseConnections.connection(name)

        return self._db[name]

    @classmethod
    def _use_replica(self, **kwargs):
        try:
            kwargs['stack'] = self.stack_mark(inspect.stack())
            sql = "select setting FROM pg_settings WHERE name = 'hot_standby'"
            r = self.instance(
                self.db_adapter().raw_query(sql=sql, **kwargs)
                ).squeeze()
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
            return self.instance(
                self.db_adapter().raw_query(
                    sql=sql, **kwargs
                    )
                ).squeeze()
        except:
            return 0

    @classmethod
    def transaction(self):
        """
        Enables multiple statements to be ran within a single transaction, see :doc:`features`.
        """
        return Transaction(self)

    @classmethod
    def column_names(self):
        """
        Returns the columns of the database table.

        :returns: list
        """
        if self.__dict__.get('_columns') is None:
            table_name = self.model_metadata()['table_name']
            columns = self.db_adapter().raw_query(
                sql="SELECT column_name FROM information_schema.columns WHERE " \
                "table_schema = 'public' AND table_name = %(table_name)s;",
                where={'table_name': table_name}
                )[0]
            self._columns = [c['column_name'] for c in columns]
        return self._columns


class ModelIterator(object):

    def __init__(self, collection):
        self.collection = collection
        self.current = 0

    def __iter__(self):
        return self

    def next(self):
        if self.current < len(self.collection):
            record = self.collection.model_class(
                **self.collection.iloc[self.current]
                )
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

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self._model.query(sql='COMMIT;', role='master')
        else:
            self._model.query(sql='ROLLBACK;', role='master')
        self._connection.autocommit = True
