from datetime import datetime
from threading import Lock

import boto3
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO

from jardin import config as config
from jardin.cache_stores.base import Base


class S3(Base):

    EXTENSION = '.feather'
    
    def __init__(self, bucket_name, path="", delete_expired_files=False):
        if not bucket_name:
            raise RuntimeError("Bucket name cannot be empty")
        self.bucket_name = bucket_name
        self.path = f"{path}/jardin_cache"
        self.delete_expired_files = delete_expired_files
        self._s3 = None
        self._lock = Lock() # guards lazy S3 init

    @property
    def s3(self):
        # Instantiate the S3 client lazily to provide greater flexibility when mocking with 'moto'.
        # Typically you wouldn't need to do this, but Jardin's cache system instantiates this class
        # at import-time which can cause initialization problems for 'moto'. We side-step the problem
        # by deferring the creation of the S3 client to first usage instead of instance init.
        if self._s3 is None:
            with self._lock:
                if self._s3 is None:
                    self._s3 = boto3.client('s3')
        return self._s3

    def __getitem__(self, key):
        if key in self:
            try:
                return pd.read_feather(BytesIO(self._get_s3_object_from_key(key)["Body"].read()))
            except Exception as ex:
                config.logging.warning(ex)
        return None

    def __setitem__(self, key, value):
        if isinstance(value, pd.DataFrame):
            try:
                with BytesIO() as _f:
                    value.to_feather(_f)
                    self.s3.put_object(Bucket=self.bucket_name,
                                       Key=self._s3_path(key),
                                       Body=_f.getvalue(),
                                       ServerSideEncryption='AES256')
            except Exception as ex:
                config.logging.warning(ex)

    def __contains__(self, key):
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=self._s3_path(key))
        except ClientError as ex:
            if ex.response['Error']['Code'] == "404":
                return False
            else:
                config.logging.warning(ex)
                return False
        else:
            return True
    
    def __delitem__(self, key):
        if self.delete_expired_files:
            try:
                self.s3.delete_object(Bucket=self.bucket_name, Key=self._s3_path(key))
            except Exception as ex:
                config.logging.warning(ex)

    def __len__(self):
        return len(self.keys())

    def keys(self):
        try:
            s3_objects = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.path)['Contents']
            return list(map(lambda x: self._key(x['Key']), s3_objects))
        except Exception as ex:
            config.logging.warning(ex)
            return []
    
    def expired(self, key, ttl=None):
        if key not in self:
            return False
        if ttl is None:
            return False
        try:
            s3_object = self._get_s3_object_from_key(key)
            if isinstance(s3_object.get("LastModified", None), datetime):
                last_modified_time = s3_object["LastModified"].replace(tzinfo=None)
                if (datetime.utcnow() - last_modified_time).seconds > ttl:
                    del self[key]
                    return True
        except Exception as ex:
            config.logging.warning(ex)
            return True
        return False
        
    def _get_s3_object_from_key(self, key):
        try:
            return self.s3.get_object(Bucket=self.bucket_name, Key=self._s3_path(key))
        except ClientError:
            return None
  
    def _s3_path(self, key):
        return f"{self.path}/{key}{self.EXTENSION}"
        
    def _key(self, s3_path):
        return s3_path.split(f"{self.path}/")[1][0:-len(self.EXTENSION)]
