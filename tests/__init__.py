from jardin.query import query


class TestTransaction(object):

    def __init__(self, model=None, create_table=True, extra_tables=[]):
        self._model = model
        self.tables = extra_tables
        if self._model:
            self._model.clear_caches()
        self._connection = DatabaseConnections.connection('jardin_test')
        self.create_table = create_table

    def setup(self):
        self.teardown()
        if self._connection.db_config.scheme == 'sqlite':
            query(
                sql='CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(256), created_at timestamp NULL, updated_at timestamp NULL, deleted_at timestamp NULL, destroyed_at timestamp NULL, num decimal);' % self._model.model_metadata()['table_name'],
                db='jardin_test'
                )
        else:
            query(
                sql='CREATE TABLE %s (id serial PRIMARY KEY, name varchar(256), created_at timestamp NULL, updated_at timestamp NULL, deleted_at timestamp NULL, destroyed_at timestamp NULL, num decimal);' % self._model.model_metadata()['table_name'],
                db='jardin_test'
                )

    def teardown(self):
        for table in self.tables + [self._model.model_metadata()['table_name']]:
            self._model.query(
                sql='DROP TABLE IF EXISTS %s;' % table
                )

    def __enter__(self):
        if self._connection.db_config.scheme == 'mysql':
            query(sql="SET sql_mode = '';", db='jardin_test')
        if self._model:
            self.teardown()
            self._model._columns = None
        self._connection.autocommit = False
        query(
            sql=self._connection.lexicon.transaction_begin_query(),
            db='jardin_test'
            )
        if self.create_table and self._model:
            self.setup()

    def __exit__(self, exc_type, exc_value, traceback):
        self._connection.connection().rollback()
        self._connection.autocommit = True


def transaction(model=None, create_table=True, extra_tables=[]):

    def decorator(func):

        def wrapper(*args, **kwargs):
            with TestTransaction(
                model,
                create_table=create_table,
                extra_tables=extra_tables
                ):
                return func(*args, **kwargs)

        return wrapper
    return decorator


from jardin.database import DatabaseConnections

def only_schemes(*schemes):

    def decorator(func):

        def wrapper(*args, **kwargs):
            if DatabaseConnections.connection('jardin_test').db_config.scheme in schemes:
                return func(*args, **kwargs)

        return wrapper
    return decorator
