from datetime import datetime
import pandas
import re, inspect
import json

import jardin.config as config
from jardin.database import DatabaseAdapter, DatabaseConnections
from jardin.tools import soft_del, classorinstancemethod, stack_marker
from jardin.transaction import Transaction
from jardin.query import query


class Collection(pandas.DataFrame):
    """
        Base class for collection of records. Inherits from `pandas.DataFrame`.
    """

    def _constructor(self, *args, **kwargs):
        instance = self.__class__(*args, **kwargs)
        if hasattr(self, 'model_class'):
            instance.model_class = self.model_class
        return instance

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


import pandas.core.reshape.concat
base_concat = pandas.core.reshape.concat.concat

def concat(*args, **kwargs):
    result = base_concat(*args, **kwargs)
    if len(args) > 0:
        for arg in args[0]:
            if isinstance(arg, Collection) and hasattr(arg, 'model_class'):
                result.model_class = arg.model_class
                break
    return result

pandas.core.reshape.concat.concat = concat
pandas.concat = concat


class RecordNotPersisted(Exception): pass


class Model(object):
    """
      Base class from which your models should inherit.
    """
    table_name = None
    table_alias = None
    db_names = {}
    has_many = []
    belongs_to = {}
    scopes = {}
    collection_class = Collection
    primary_key = 'id'
    soft_delete = False

    def __init__(self, **kwargs):
        self.attributes = dict()
        table_schema = self.__class__.table_schema()
        self.attributes[self.primary_key] = kwargs.get(self.primary_key, None)
        for column in set(list(table_schema.keys()) + list(kwargs.keys())):
            self.attributes[column] = kwargs.get(
                column,
                table_schema.get(column, {}).get('default'))
            # MySQL filth
            if self.attributes[column] == '0000-00-00 00:00:00':
                self.attributes[column] = None
            if column in kwargs:
                del kwargs[column]
        self.init_relationships()
        super(Model, self).__init__(**kwargs)

    def __getattribute__(self, i):
        try:
            return super(Model, self).__getattribute__(i)
        except AttributeError as e:
            if i == 'attributes':
                return dict()
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
        if self.primary_key in self.attributes:
            attrs += ['%s=%s' % (self.primary_key, self.id)]
        for (att_name, attr_value) in self.attributes.items():
            if att_name == self.primary_key: continue
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


    # Individual DB methods

    @property
    def where_self(self):
        return {self.primary_key: getattr(self, self.primary_key)}

    @classmethod
    def deleted_at_column(self):
        if self.soft_delete is True:
            return 'deleted_at'
        if isinstance(self.soft_delete, str):
            return self.soft_delete

    def save(self):
        if self.persisted:
            return self.__class__.update(
                values=self,
                where=self.where_self
                )
        else:
            self.attributes = self.__class__.insert(values=self).attributes

    def destroy(self, force=False):
        """
        Deletes the record. If the model has ``soft_delete`` activated, the record will not actually be deleted.

        :param force: forces the record to be actually deleted if ``soft_delete`` is activated.
        :type force: boolean
        """
        if self.persisted:
            if self.soft_delete and not force:
                self.__class__.update(
                    values={
                        self.__class__.deleted_at_column(): datetime.utcnow()
                        },
                    where=self.where_self
                    )
            else:
                self.__class__.delete(
                    where=self.where_self,
                    skip_soft_delete=True
                    )
        else:
            raise RecordNotPersisted("Record's primary key is None")

    def reload(self):
        self.__init__(
            **self.__class__.find(
                self.attributes[self.primary_key]
                ).attributes
            )

    @property
    def persisted(self):
        return self.attributes.get(self.primary_key) is not None

    def init_relationships(self):
        this_table_name = self._table_name()
        for h in self.has_many:
            other_table_name = h._table_name()
            def func(slf, **kwargs):
                where = kwargs.get('where', {})
                where.update(**{
                    h.belongs_to[this_table_name]: getattr(slf, slf.primary_key)
                    })
                kwargs['where'] = where
                return h.select(**kwargs)
            setattr(self.__class__, other_table_name, func)

    @classmethod
    def collection_instance(self, result=None):
        if isinstance(result, list) and len(result) and isinstance(result[0], dict):
            collection = self.collection_class.from_records(result)
        else:
            collection = self.collection_class(result)
        collection.model_class = self
        return collection

    @classmethod
    def stack_mark(self, stack, db_conn=None):
        return stack_marker(stack, db_conn=db_conn)

    @classmethod
    @soft_del
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
        :param having: Specifies a HAVING clause.
        :type having: string
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
            role=kwargs.get('role', 'replica')
            )

        kwargs['stack'] = self.stack_mark(
            inspect.stack(),
            db_conn=db_adapter.db
            )

        return self.collection_instance(db_adapter.select(**kwargs))

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
        :returns: ``jardin.Collection`` collection, which is a ``pandas.DataFrame``.
        """
        results = query(
            sql=sql,
            filename=filename,
            db=self.db_names[kwargs.get('role', 'replica')],
            **kwargs
            )

        if results is None:
            return None
        else:
            return self.collection_instance(results)

    @classmethod
    @soft_del
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
            kwargs['select'] = {'cnt': 'COUNT(%s)' % kwargs['select']}
        else:
            kwargs['select'] = {'cnt': 'COUNT(*)'}

        res = self.db_adapter(
            db_name=kwargs.get('db'),
            role=kwargs.get('role', 'replica')
            ).select(**kwargs)
        return res.cnt[0]

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
            for (k, v) in list(values.items()):
                if v is None:
                    del kwargs['values'][k]
            
        kwargs['stack'] = self.stack_mark(inspect.stack())
        kwargs['primary_key'] = self.primary_key

        column_names = self.table_schema().keys()
        now = datetime.utcnow()
        for field in [f for f in ('created_at', 'updated_at') if f in column_names]:
            if isinstance(kwargs['values'], dict):
                kwargs['values'][field] = kwargs['values'].get(field, now)
            else:
                kwargs['values'][field] = now
        results = self.db_adapter(role='master').insert(**kwargs)
        return self.record_or_model(results)

    @classmethod
    def record_or_model(self, results):
        if results is None:
            return None
        if len(results) == 1:
            return self(**results.to_dict(orient='records')[0])
        else:
            return self.collection_instance(results)

    @classmethod
    @soft_del
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
        column_names = self.table_schema().keys()
        now = datetime.utcnow()
        if 'updated_at' in column_names:
            if 'updated_at' not in kwargs['values']:
                kwargs['values']['updated_at'] = now
        results = self.db_adapter(role='master').update(**kwargs)
        return self.record_or_model(results)

    @classorinstancemethod
    def touch(self, **kwargs):
        if type(self) == type:
            kwargs['values'] = {'updated_at': datetime.utcnow()}
            return self.update(**kwargs)
        else:
            self.__class__.touch(where=self.where_self)
            self.reload()


    @classmethod
    @soft_del
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
        return self.collection_instance(
            self.db_adapter(
                db_name=kwargs.get('db'),
                role=kwargs.get('role', 'replica')
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
            return self(
                **self.select(
                    where=values,
                    limit=1,
                    **kwargs
                    ).to_dict(orient='records')[0]
                )
        except IndexError:
            return None

    @classmethod
    def find(self, id, **kwargs):
        """
        Finds a record by its id in the model's table in the replica database.
        :returns: an instance of the model.
        """
        return self.find_by(values={self.primary_key: id}, **kwargs)

    @classmethod
    def db_adapter(self, role='replica', db_name=None):
        if not hasattr(self, '_db_metadata'):
            self._db_metadata = {}
        db_name = db_name or self.db_names.get(role)
        key = '%s_%s' % (db_name, role)
        if key not in self._db_metadata:
            self._db_metadata[key] = self.model_metadata()
        return DatabaseAdapter(
            self.db(role=role, db_name=db_name),
            self._db_metadata[key]
            )

    @classmethod
    def model_metadata(self, include_schema=True):
        metadata = {
            'table_name': self._table_name(),
            'table_alias': self._table_alias(),
            'belongs_to': self.belongs_to,
            'scopes': self.scopes
        }
        if include_schema:
            metadata['table_schema'] = self.table_schema()
        return metadata

    @staticmethod
    def _default_table_alias(table_name):
        return ''.join([w[0] for w in table_name.split('_')])

    @classmethod
    def _table_name(self):
        if self.__dict__.get('__table_name') is None:
            self.__table_name = self.table_name or self._default_table_name()
        return self.__table_name

    @classmethod
    def _table_alias(self):
        if self.__dict__.get('__table_alias') is None:
            self.__table_alias = self.table_alias or self._default_table_alias(
                self._table_name()
                )
        return self.__table_alias

    @classmethod
    def _default_table_name(self):
        import inflect
        name = self.__name__
        s1 = re.sub('([A-Z])', r'_\1', name)[1:]
        s1 = s1.split('_')
        s1 = list(map(lambda x: x.lower(), s1))
        s1[-1] = inflect.engine().plural_noun(s1[-1])
        return '_'.join(s1)

    @classmethod
    def db(self, role='replica', db_name=None):
        name = db_name or self.db_names.get(role)
        return DatabaseConnections.connection(name)

    @classmethod
    def _use_replica(self, **kwargs):
        try:
            kwargs['stack'] = self.stack_mark(inspect.stack())
            sql = "select setting FROM pg_settings WHERE name = 'hot_standby'"
            r = self.collection_instance(
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
            return self.collection_instance(
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
        return Transaction(self.db(role='master').name)

    @classmethod
    def table_schema(self):
        """
        Returns the table schema.

        :returns: dict
        """
        if self.__dict__.get('_table_schema') is None:
            self._table_schema = None
            table_schema = {}
            for row in self.query_schema():
                name, default, dtype = self.db().lexicon.column_info(row)
                if isinstance(default, str):
                    json_matches = re.findall(r"^\'(.*)\'::jsonb$", default)
                    if len(json_matches) > 0:
                        default = json.loads(json_matches[0])
                if name == self.primary_key:
                    default = None
                table_schema[name] = {'default': default, 'type': dtype}
            if len(table_schema):
                self._table_schema = table_schema
        return self._table_schema

    @classmethod
    def query_schema(self):
        db_adapter = DatabaseAdapter(
            self.db(role='replica', db_name=self.db_names.get('replica')),
            self.model_metadata(include_schema=False)
            )
        return db_adapter.raw_query(
            sql=db_adapter.db.lexicon.table_schema_query(self._table_name()),
            where={'table_name': self._table_name()}
            ).to_dict(orient='records')

    @classmethod
    def clear_caches(self):
        self._table_schema = None
        self._db_metadata = {}
        self.__table_alias = None
        self.__table_name = None

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
