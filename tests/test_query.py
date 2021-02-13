import os
import unittest
from unittest.mock import patch
import jardin
import jardin
from pandas._testing import assert_frame_equal
from tests import transaction
from tests.models import JardinTestModel
class User(JardinTestModel):
    pass

class TestQuery(unittest.TestCase):

    @transaction(model=User)
    def test_query_sql(self):
        User.insert(values={'name': 'jardin'})
        df = jardin.query(
            sql='SELECT * FROM users;',
            db='jardin_test'
            )
        self.assertEqual(len(df), 1)
        self.assertEqual(df.name.iloc[0], 'jardin')

    @transaction(model=User)
    def test_query_params(self):
        if User.db().db_config.scheme == 'sqlite':
            return
        User.insert(values={'name': 'jardin'})
        df = jardin.query(
            sql='SELECT * FROM users WHERE name IN %(names)s;',
            params={'names': ['jardin']},
            db='jardin_test'
            )
        self.assertEqual(len(df), 1)

    @transaction(model=User)
    def test_query_filename_relative(self):
        User.insert(values={'name': 'jardin'})
        df = jardin.query(
            filename='tests/support/select_users.sql',
            db='jardin_test'
            )
        self.assertEqual(len(df), 1)
        self.assertEqual(df.name.iloc[0], 'jardin')

    @transaction(model=User)
    def test_query_filename_absolute(self):
        User.insert(values={'name': 'jardin'})
        filename = os.path.join(os.environ['PWD'], 'tests/support/select_users.sql')
        df = jardin.query(
            filename=filename,
            db='jardin_test'
            )
        self.assertEqual(len(df), 1)
        self.assertEqual(df.name.iloc[0], 'jardin')

    def test_snowflake_lexicon(self):
        from jardin.database.clients.sf import Lexicon
        sql, params = Lexicon.standardize_interpolators(
            'SELECT * FROM users WHERE a = %(abc)s AND b = %(def)s',
            {'def': 2, 'abc': 1}
            )
        self.assertEqual(
            'SELECT * FROM users WHERE a = %s AND b = %s',
            sql
            )
        self.assertEqual([1, 2], params)
    
    @transaction(model=User)
    def test_query_cached(self):
        cache = jardin.cache_stores.cache_store
        for key in cache.keys():
            del cache[key]
        User.insert(values={'name': 'jardin'})
        with patch('jardin.database.base_client.BaseClient') as mock:
            df1 = jardin.query("select * from users", db="jardin_test", cache=True, ttl=10)
            df2 = jardin.query("select * from users", db="jardin_test", cache=True, ttl=10)
            assert_frame_equal(df1, df2, check_like=True)
            self.assertEqual(mock.execute.call_count, 1)

if __name__ == "__main__":
    unittest.main()
