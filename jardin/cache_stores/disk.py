import os
import pyarrow.feather as feather
import pandas as pd
import time
from datetime import datetime

from jardin.cache_stores.base import Base

class Disk(Base):
    
    EXTENSION = '.feather'

    def __init__(self, dir, limit=None, ttl=None):
        
        """
            dir: path of directory to use for cached files
            limit: size limit in bytes for lru cache
            ttl: cache expiry in seconds
        """
        
        self.dir = dir
        self.limit = limit
        self.ttl = ttl
        if not os.path.exists(self.dir):
            try:
                os.makedirs(self.dir)
            except FileExistsError as ex:
                pass

    def __getitem__(self, key):
        if key in self:
            return feather.read_feather(self._path(key))
        return None

    def __setitem__(self, key, value):
        if not isinstance(value, pd.DataFrame):
            return None
        
        feather.write_feather(value, self._path(key))

        if self.limit is not None:
            if os.path.getsize(self._path(key)) > self.limit:
                raise MemoryError(f"disk cache limit exceeded by single key {key}")
            while self.size() > self.limit:
                del self[self.lru()]

    def __delitem__(self, key):
        if os.path.exists(self._path(key)):
            os.remove(self._path(key))

    def __contains__(self, key):
        f = self._path(key)
        if not os.path.isfile(f):
            return False
        if self.ttl is None:
            return True
        return int(time.time() - os.stat(f).st_mtime) < self.ttl

    def __len__(self):
        return len(self.keys())

    def size(self):
        return sum(os.path.getsize(f) for f in self.values())

    def keys(self):
        return [self._key(f) for f in os.listdir(self.dir)]

    def values(self):
        return [os.path.join(self.dir, f) for f in os.listdir(self.dir)]

    def lru(self):
        files = sorted(self.values(), key=lambda f: os.stat(f).st_atime)
        if not files:
            return None
        return self._key(files[0])

    def _path(self, key):
        return os.path.join(self.dir, key + self.EXTENSION)

    def _key(self, path):
        return os.path.basename(path)[0:-len(self.EXTENSION)]