from jardin.database.client_provider import ClientProvider
from jardin.query import query


class TestTransaction(object):

    def __init__(self, model=None, create_table=True, extra_tables=[]):
        self._model = model
        self.tables = extra_tables
        if self._model:
            self._model.clear_caches()
        self._db_config = Datasources.db_config('jardin_test')
        self._connection = ClientProvider('jardin_test').next_client()
        self.create_table = create_table

    def setup(self):
        if self.create_table and self._model:
            self.teardown()
            if self._db_config.scheme == 'sqlite':
                query(
                    sql='CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(256), created_at timestamp NULL, updated_at timestamp NULL, deleted_at timestamp NULL, destroyed_at timestamp NULL, num decimal);' % self._model._table_name(),
                    db='jardin_test'
                    )
            else:
                query(
                    sql='CREATE TABLE %s (id serial PRIMARY KEY, name varchar(256), created_at timestamp NULL, updated_at timestamp NULL, deleted_at timestamp NULL, destroyed_at timestamp NULL, num decimal);' % self._model._table_name(),
                    db='jardin_test'
                    )

    def teardown(self):
        Datasources.unban_all_clients('jardin_test')
        if self._model:
            for table in self.tables + [self._model._table_name()]:
                self._model.query(
                    sql='DROP TABLE IF EXISTS %s;' % table
                    )
            self._model._columns = None

    def __enter__(self):
        if self._db_config.scheme == 'mysql':
            query(sql="SET sql_mode = '';", db='jardin_test')
        self.teardown()
        self.setup()

    def __exit__(self, exc_type, exc_value, traceback):
        pass


def transaction(model=None, create_table=True, extra_tables=[]):

    def decorator(func, *args, **kwargs):

        def wrapper(*args, **kwargs):
            with TestTransaction(
                model,
                create_table=create_table,
                extra_tables=extra_tables
                ):
                return func(*args, **kwargs)

        return wrapper
    return decorator


from jardin.database.datasources import Datasources


def only_schemes(*schemes):

    def decorator(func):

        def wrapper(*args, **kwargs):
            if Datasources.db_config('jardin_test') in schemes:
                return func(*args, **kwargs)

        return wrapper
    return decorator
