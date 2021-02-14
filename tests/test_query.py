import os
import unittest
from unittest.mock import patch
import jardin
import time
import pandas as pd
from pandas._testing import assert_frame_equal
from tests import transaction
from tests.models import JardinTestModel
from jardin.database.base_client import BaseClient

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
    
    def test_query_cached(self):
        cache = jardin.cache_stores.cache_store
        for key in cache.keys():
            del cache[key]
        results, columns = [{"a": 1}], ["a"]
        with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
            df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True)
            df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True)
            assert_frame_equal(df1, df2, check_like=True)
            time.sleep(2)
            df3 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, ttl=1)
            self.assertEqual(mock_method.call_count, 2)

if __name__ == "__main__":
    unittest.main()
