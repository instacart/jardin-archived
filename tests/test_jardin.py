import unittest
from mock import patch
from time import sleep
from freezegun import freeze_time
from datetime import datetime, timedelta
import pandas as pd

from tests import transaction
from tests.models import JardinTestModel
from support.mydatetime import _mydatetime

class User(JardinTestModel): pass


class TestModel(unittest.TestCase):

    @patch('pandas.datetime', _mydatetime) #hack to fix https://github.com/spulec/freezegun/issues/242
    @transaction(model=User)
    def test_created_at_updated_at(self):
        user = User.insert(values={'name': 'Jardinier'})
        user = User.find(user.id)
        user2 = User.insert(values={'name': 'Jardinier 2'})
        self.assertNotEqual(user.name, user2.name)
        self.assertIsNotNone(user.created_at)
        self.assertIsNotNone(user.updated_at)
        with freeze_time(datetime.utcnow() + timedelta(hours=1)):
            User.update(
                values={'name': 'Jardinier 3'},
                where={'id': user.id}
                )
        updated_user = User.find(user.id)
        self.assertTrue(updated_user.updated_at > user.updated_at)
        self.assertEqual(updated_user.created_at, user.created_at)

    @transaction(model=User, create_table=False)
    def test_no_created_at_updated_at(self):
        if User.db().db_config.scheme == 'sqlite':
            User.query(
                sql='CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(256));'
                )
        else:
            User.query(
                sql='CREATE TABLE users (id serial PRIMARY KEY, name varchar(256));'
                )
        user = User.insert(values={'name': 'Jardinier'})
        self.assertEqual(User.count(), 1)
        self.assertFalse('updated_at' in user.attributes)
        self.assertFalse('created_at' in user.attributes)

    @transaction(model=User)
    def test_empty_insert(self):
        User.insert(values={})
        self.assertEqual(User.count(), 0)
        User.insert(values=pd.DataFrame())
        self.assertEqual(User.count(), 0)

    @transaction(model=User)
    def test_count(self):
        self.assertEqual(User.count(), 0)
        User.insert(values={'name': 'Holberton'})
        self.assertEqual(User.count(), 1)

    @transaction(model=User)
    def test_custom_count(self):
        User.insert(values={'name': 'Holberton'})
        User.insert(values={'name': 'Holberton'})
        self.assertEqual(User.count(select='DISTINCT(name)'), 1)

    #def test_index_by(self):
    #    users = User({'name': ['John', 'Paul']})
    #    indexed = users.index_by('name')
    #    self.assertEqual(len(indexed), 2)
    #    self.assertIsInstance(indexed['John'], Users.record_class)
    #    self.assertIsInstance(indexed['Paul'], Users.record_class)
    #    self.assertEqual(indexed['Paul'].name, 'Paul')


if __name__ == "__main__":
    unittest.main()
