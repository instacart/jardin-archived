import datetime
import unittest
import os
import time
import jardin
import pandas as pd
from pandas._testing import assert_frame_equal
from jardin.cache_stores.disk import Disk
from jardin.cache_stores import cache_store

class TestDisk(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def test_disk(self):
        cache = Disk(dir=os.path.join('/tmp', 'cache'))
        
        for key in cache.keys():
            del cache[key]
            
        self.assertEqual(len(cache), 0)
        self.assertEqual(cache['a'], None)
        self.assertEqual(cache.keys(), [])

        test_df = pd.DataFrame([{"a": 1}])
            
        cache['a'] = test_df
        self.assertEqual(len(cache), 1)
        assert_frame_equal(cache['a'], test_df)
        self.assertEqual(cache.keys(), ['a'])
        
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
        
        cache.ttl = 1
        cache.limit = None
        cache['a'] = test_df
        time.sleep(2)
        self.assertFalse('a' in cache)
        
        cache.ttl = None
