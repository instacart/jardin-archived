import pytest
from unittest.mock import patch
import datetime
import boto3
import os
import time
import jardin
import pandas as pd
from pandas.testing import assert_frame_equal
from jardin.database.base_client import BaseClient
from jardin.cache_stores.s3 import S3
from moto import mock_s3
from contextlib import contextmanager
from tests.models import JardinTestModel

class User(JardinTestModel):
    pass

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@mock_s3
@pytest.fixture(scope='function')
def s3(aws_credentials):
    yield boto3.client('s3')


@contextmanager
def setup_s3(s3, bucket_name):
    with mock_s3():
        s3.create_bucket(Bucket=bucket_name)
        yield
      
def test_s3_cache(s3):
    with setup_s3(s3, "JARDIN_BUCKET"):
        cache = S3(bucket_name="JARDIN_BUCKET", path="cache")
        cache.clear() # clear the cache
        
        assert len(cache) == 0
        
        test_df = pd.DataFrame([{"A": 1}], columns=["A"])
        cache["A"] = test_df
        assert len(cache) == 1
        assert_frame_equal(cache["A"], test_df, check_like=True)
        
        cache.clear()
        
def test_query_cache_with_s3(s3):
    User.insert(values={'name': 'jardin_s3'})
    with setup_s3(s3, "JARDIN_BUCKET"):
        cache = S3(bucket_name="JARDIN_BUCKET", path="cache")
        cache.clear() # clear the cache
        
        results, columns = [{"a": 1}], ["a"]
        
        # when caching is not configured
        jardin.cache_stores.STORES["s3"] = None
        with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
            df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="s3")
            df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="s3")
            assert_frame_equal(df1, df2, check_like=True)
            assert mock_method.call_count == 2
    
        # when caching is configured
        jardin.cache_stores.STORES["s3"] = cache
        with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
            df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="s3")
            df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="s3")
            assert_frame_equal(df1, df2, check_like=True)
            assert mock_method.call_count == 1
        cache.clear()
        
        # with ttl
        jardin.cache_stores.STORES["s3"] = cache
        with patch.object(BaseClient, 'execute', return_value=(results, columns)) as mock_method:
            df1 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="s3")
            time.sleep(2)
            df2 = jardin.query("select * from users limit 10", db="jardin_test", cache=True, cache_method="s3", ttl=1)
            assert_frame_equal(df1, df2, check_like=True)
            assert mock_method.call_count == 2
        