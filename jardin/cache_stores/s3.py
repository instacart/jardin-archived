from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO
from pyarrow.feather import write_feather

from jardin.cache_stores.base import Base
class S3(Base):

    EXTENSION = '.feather'
    
    def __init__(self, bucket_name, path):
        self.bucket_name = bucket_name
        self.path = path
        self.s3 = boto3.resource('s3')
        
    def __getitem__(self, key):
        if key in self:
            try:
                return pd.read_feather(BytesIO(self._get_s3_object_from_key(key).read()))
            except BaseException:
                return None
        return None

    def __setitem__(self, key, value):
        if isinstance(value, pd.DataFrame):
            with BytesIO() as _f:
                value.to_feather(_f)
                self.s3.Bucket(self.bucket_name).put_object(Key=self._s3_key(key),
                                                            Body=_f.getvalue(),
                                                            ServerSideEncryption='AES256')

    def __contains__(self, key):
        try:
            self.s3.Object(self.bucket_name, self._s3_key(key)).load()
        except ClientError as ex:
            if ex.response['Error']['Code'] == "404":
                return False
            else:
                print("something got wrong")
                return False
        else:
            return True
    
    def __delitem__(self, key):
        print("delete key")
        pass

    def __len__(self):
        return len(self.keys())

    def keys(self):
        s3_client = boto3.client('s3')
        return s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.path)['Contents']
    
    def expired(self, key, ttl=None):
        if key not in self:
            return False
        if ttl is None:
            return False
        s3_object = self._get_s3_object_from_key(key)
        if isinstance(s3_object.get("LastModified", None), datetime):
            last_modified_time = s3_object["LastModified"].replace(tzinfo=None)
            if (datetime.utcnow() - last_modified_time).seconds > ttl:
                del self[key]
                return True
        return False
        
    def _get_s3_object_from_key(self, key):
        try:
            return self.s3.Object(self.bucket_name, self._s3_key(key)).get()
        except ClientError as ex:
            return None
  
    def _s3_key(self, key):
        return f"{self.path}/{key}{self.EXTENSION}"
