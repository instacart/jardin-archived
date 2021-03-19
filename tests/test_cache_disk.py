import datetime
import unittest
from unittest.mock import patch
import boto3
import os
import time
import jardin
import pandas as pd
from concurrent import futures
from pandas.testing import assert_frame_equal
from jardin.database.base_client import BaseClient
from jardin.cache_stores.disk import Disk
from tests.models import JardinTestModel
from tests import TestTransaction   

class User(JardinTestModel):
    pass

class TestDisk(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def test_disk(self):
        cache = Disk(dir=os.path.join('/tmp', 'cache'))
        cache.clear() # clear the cache
            
        self.assertEqual(len(cache), 0)
        self.assertEqual(cache['a'], None)
        self.assertEqual(cache.keys(), [])

        test_df = pd.DataFrame([{"a": 1}])
            
        cache['a'] = test_df
        self.assertEqual(len(cache), 1)
        assert_frame_equal(cache['a'], test_df)
        self.assertEqual(cache.keys(), ['a'])
        
        time.sleep(1)
        
        cache['b'] = test_df
        self.assertEqual(len(cache), 2)
        self.assertEqual(cache.lru(), 'a')
        self.assertEqual(sorted(cache.keys()), ['a', 'b'])

        del cache['b']
        self.assertEqual(len(cache), 1)
        self.assertEqual(cache.lru(), 'a')
        self.assertFalse('b' in cache)
        self.assertEqual(cache.keys(), ['a'])

        cache.limit = 0
        self.assertRaises(MemoryError, cache.__setitem__, 'a', test_df)
        del cache['a']

        self.assertEqual(len(cache), 0)
        self.assertEqual(cache.lru(), None)
        self.assertFalse('a' in cache)
        self.assertEqual(cache.keys(), [])
        
        cache.clear() # clear the cache
    
    def test_cache_query_with_disk(self):
        with TestTransaction(User):
            User.insert(values={'name': 'jardin_disk'})
            results, columns = [{"a": 1}], ["a"]
            
            # when caching is not configured
            jardin.cache_stores.STORES["disk"] = None
            with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
                df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                assert_frame_equal(df1, df2, check_like=True)
                self.assertEqual(mock_method.call_count, 2)
            
            # when caching is badly configured
            with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
                df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                assert_frame_equal(df1, df2, check_like=True)
                self.assertEqual(mock_method.call_count, 2)

            # when caching is configured
            jardin.cache_stores.STORES["disk"] = Disk(dir="/tmp/jardin_cache")
            jardin.cache_stores.STORES["disk"].clear()
            with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
                df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                assert_frame_equal(df1, df2, check_like=True)
                self.assertEqual(mock_method.call_count, 1)
            jardin.cache_stores.STORES["disk"].clear()

            # with ttl
            with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
                df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                time.sleep(2)
                df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk", ttl=1)
                assert_frame_equal(df1, df2, check_like=True)
                self.assertEqual(mock_method.call_count, 2)
                
                
    def test_multi_threading_cache_query_with_disk(self):
        with TestTransaction(User):
            User.insert(values={'name': 'jardin_disk'})
            results, columns = [{"a": 1}], ["a"]
            
            jardin.cache_stores.STORES["disk"] = Disk(dir="/tmp/jardin_cache")
            jardin.cache_stores.STORES["disk"].clear()
            with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
                def run_query():
                    df = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="disk")
                with futures.ThreadPoolExecutor(max_workers=1) as pool:
                    tasks = [pool.submit(run_query) for _ in range(10)]
                    for task in tasks:
                        task.result()
            self.assertEqual(mock_method.call_count, 1)
            jardin.cache_stores.STORES["disk"].clear()