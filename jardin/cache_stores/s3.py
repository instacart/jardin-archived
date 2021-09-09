from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO

from memoized_property import memoized_property
from pyarrow.feather import write_feather

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

    @memoized_property
    def s3(self):
        # Instantiate the S3 resource lazily to provide greater flexibility when mocking with 'moto'.
        # Typically you wouldn't need to do this, but Jardin's cache system instantiates this class
        # at import-time which can cause initialization problems for 'moto'. We side-step the problem
        # by deferring the creation of the S3 resource to first usage instead of instance init.
        return boto3.resource('s3')

    def check_valid_bucket(self):
        try:
            self.s3.meta.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as ex:
            config.logging.warning(f"Bucket {self.bucket_name} does not exist")
            raise ex
            
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
                    self.s3.Bucket(self.bucket_name).put_object(Key=self._s3_path(key),
                                                                Body=_f.getvalue(),
                                                                ServerSideEncryption='AES256')
            except Exception as ex:
                config.logging.warning(ex)

    def __contains__(self, key):
        try:
            self.s3.Object(self.bucket_name, self._s3_path(key)).load()
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
                self.s3.Object(self.bucket_name, self._s3_path(key)).delete()
            except Exception as ex:
                config.logging.warning(ex)

    def __len__(self):
        return len(self.keys())

    def keys(self):
        try:
            s3_objects = self.s3.meta.client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.path)['Contents']
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
            return self.s3.Object(self.bucket_name, self._s3_path(key)).get()
        except ClientError as ex:
            return None
  
    def _s3_path(self, key):
        return f"{self.path}/{key}{self.EXTENSION}"
        
    def _key(self, s3_path):
        return s3_path.split(f"{self.path}/")[1][0:-len(self.EXTENSION)]
        
