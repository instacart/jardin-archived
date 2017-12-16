import unittest
import pandas as pd

from models import JardinTestModel


class Users(JardinTestModel): pass


class TestTransaction(object):

    def __init__(self, model, create_table=True):
        self._model = model
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
                sql='CREATE TABLE %s (id serial PRIMARY KEY, name varchar(256), created_at timestamp, updated_at timestamp);' % self._model.model_metadata()['table_name']
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

class TestModel(unittest.TestCase):

    @transaction(model=Users)
    def test_created_at_updated_at(self):
        user = Users.insert(values={'name': 'Jardinier'})
        self.assertIsNotNone(user.created_at)
        self.assertIsNotNone(user.updated_at)
        updated_user = Users.update(
            values={'name': 'Jardinier 2'},
            where={'id': user.id}
            )
        self.assertTrue(updated_user.updated_at > user.updated_at)
        self.assertEqual(updated_user.created_at, user.created_at)

    @transaction(model=Users, create_table=False)
    def test_no_created_at_updated_at(self):
        Users.query(
            sql='CREATE TABLE users (id serial PRIMARY KEY, name varchar(256));'
            )
        user = Users.insert(values={'name': 'Jardinier'})
        self.assertEqual(Users.count(), 1)
        self.assertFalse('updated_at' in user)
        self.assertFalse('created_at' in user)

    @transaction(model=Users)
    def test_empty_insert(self):
        Users.insert(values={})
        self.assertEqual(Users.count(), 0)
        Users.insert(values=pd.DataFrame())
        self.assertEqual(Users.count(), 0)

    @transaction(model=Users)
    def test_count(self):
        self.assertEqual(Users.count(), 0)
        Users.insert(values={'name': 'Holberton'})
        self.assertEqual(Users.count(), 1)

    @transaction(model=Users)
    def test_custom_count(self):
        Users.insert(values={'name': 'Holberton'})
        Users.insert(values={'name': 'Holberton'})
        self.assertEqual(Users.count(select='DISTINCT(name)'), 1)

    def test_index_by(self):
        users = Users({'name': ['John', 'Paul']})
        indexed = users.index_by('name')
        self.assertEqual(len(indexed), 2)
        self.assertIsInstance(indexed['John'], Users.record_class)
        self.assertIsInstance(indexed['Paul'], Users.record_class)
        self.assertEqual(indexed['Paul'].name, 'Paul')

    @transaction(model=Users)
    def test_select(self):
        Users.insert(values={'name': 'George'})
        users = Users.select(select={'name': 'custom_name'})
        self.assertEqual(users.custom_name[0], 'George')


if __name__ == "__main__":
    unittest.main()
