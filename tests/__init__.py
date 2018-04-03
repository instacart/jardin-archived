from tests.models import JardinTestModel


class TestTransaction(object):

    def __init__(self, model, create_table=True, extra_tables=[]):
        self._model = model
        self.tables = extra_tables
        self._model.clear_caches()
        self._connection = self._model.db(role='master')
        self.create_table = create_table

    def setup(self):
        self.teardown()
        if self._model.db().db_config.scheme == 'sqlite':
           self._model.query(
                sql='CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(256), created_at timestamp NULL, updated_at timestamp NULL, deleted_at timestamp NULL, destroyed_at timestamp NULL, num decimal);' % self._model.model_metadata()['table_name']
                )
        else:
            self._model.query(
                sql='CREATE TABLE %s (id serial PRIMARY KEY, name varchar(256), created_at timestamp NULL, updated_at timestamp NULL, deleted_at timestamp NULL, destroyed_at timestamp NULL, num decimal);' % self._model.model_metadata()['table_name']
                )

    def teardown(self):
        for table in self.tables + [self._model.model_metadata()['table_name']]:
            self._model.query(
                sql='DROP TABLE IF EXISTS %s;' % table
                )

    def __enter__(self):
        if self._model.db().db_config.scheme == 'mysql':
            self._model.query(sql="SET sql_mode = '';")
        self.teardown()
        self._model._columns = None
        self._connection.autocommit = False
        self._model.query(
            sql=self._model.db().lexicon.transaction_begin_query(),
            role='master'
            )
        if self.create_table:
            self.setup()

    def __exit__(self, exc_type, exc_value, traceback):
        self._model.db(role='master').connection().rollback()
        self._connection.autocommit = True


def transaction(model=JardinTestModel, create_table=True, extra_tables=[]):

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
