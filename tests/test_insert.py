import unittest
from datetime import datetime
import pandas
import numpy
from tests import transaction, only_schemes
from tests.models import JardinTestModel
from jardin.query import query


class User(JardinTestModel):
    pass

class TestInsert(unittest.TestCase):

    @transaction(model=User)
    def test_nat(self):
        df = pandas.DataFrame(columns=['name', 'deleted_at', 'num'])
        df = df.append([{'name': 'user', 'deleted_at': None, 'num': numpy.nan}])
        df = df.append([{'name': 'user2', 'deleted_at': None, 'num': numpy.nan}])
        df['deleted_at'] = pandas.to_datetime(df.deleted_at)
        User.insert(values=df)
        self.assertEqual(User.count(), len(df))

    @transaction(model=User)
    def test_single_insert(self):
        User.insert(values={'name': 'user3', 'deleted_at': None})

    @transaction(model=User)
    def test_single_insert_with_id(self):
        user = User.insert(values={'name': 'user999', 'id': 999})
        self.assertEqual(user.id, 999)

    @transaction(model=User)
    def test_single_insert_with_created_at(self):
        now = '2020-01-01 00:00:00'
        user = User.insert(values={'name': 'user4', 'created_at': now}) 
        self.assertEqual(str(user.created_at), now)

    @only_schemes('postgres')
    @transaction(create_table=False)
    def test_pg_array(self):
        User.clear_caches()
        query('CREATE TABLE users (id SERIAL PRIMARY KEY, ids TEXT ARRAY);', db='jardin_test')
        self.assertEqual(User.count(), 0)
        User.insert(values={'ids': ['a', 'b', 'c']})
        self.assertEqual(User.count(), 1)

    @only_schemes('postgres')
    @transaction(create_table=False)
    def test_pg_jsonb_list(self):
        User.clear_caches()
        query('CREATE TABLE users (id SERIAL PRIMARY KEY, ids JSONB);', db='jardin_test')
        self.assertEqual(User.count(), 0)
        User.insert(values={'ids': ['a', 'b', 'c']})
        self.assertEqual(User.count(), 1)

if __name__ == "__main__":
    unittest.main()
