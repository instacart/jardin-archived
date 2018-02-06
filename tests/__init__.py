from models import JardinTestModel


class TestTransaction(object):

    def __init__(self, model, create_table=True):
        self._model = model
        self._model.clear_caches()
        self._connection = self._model.db(role='master')
        self.create_table = create_table

    def __enter__(self):
        self._model.query(
            sql='drop table if exists %s cascade;' % self._model.model_metadata()['table_name']
            )
        self._model._columns = None
        self._connection.autocommit = False
        self._model.query(sql='BEGIN;', role='master')
        if self.create_table:
            self._model.query(
                sql='CREATE TABLE %s (id serial PRIMARY KEY, name varchar(256), created_at timestamp, updated_at timestamp, deleted_at timestamp, destroyed_at timestamp);' % self._model.model_metadata()['table_name']
                )

    def __exit__(self, exc_type, exc_value, traceback):
        self._model.query(sql='ROLLBACK;', role='master')
        self._connection.autocommit = True


def transaction(model=JardinTestModel, create_table=True):

    def decorator(func):

        def wrapper(*args, **kwargs):
            with TestTransaction(model, create_table=create_table):
                return func(*args, **kwargs)

        return wrapper
    return decorator
