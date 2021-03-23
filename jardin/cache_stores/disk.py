import os
import gc
import pyarrow.feather as feather
import pandas as pd
import time
from datetime import datetime
import threading

from jardin import config as config
from jardin.cache_stores.base import Base

try:
    FileExistsError
except NameError:
    FileExistsError = OSError

class Disk(Base):
    
    EXTENSION = '.feather'

    def __init__(self, dir="/tmp", limit=None):
        
        """
            dir: path of directory to use for cached files
            limit: size limit in bytes for lru cache
            ttl: cache expiry in seconds
        """
        
        self.dir = f"{dir}/jardin_cache"
        self.limit = limit
        self._lock = threading.Lock()
        if not os.path.exists(self.dir):
            try:
                os.makedirs(self.dir)
            except FileExistsError as ex:
                raise ex

    def __getitem__(self, key):
        with self._lock:
            if key in self:
                try:
                    return feather.read_feather(self._path(key))
                except Exception as ex:
                    config.logging.warning(ex)
                    del self[key]
        return None

    def __setitem__(self, key, value):
        if not isinstance(value, pd.DataFrame):
            return None
    
        with self._lock:
            feather.write_feather(value, self._path(key))

            if self.limit is not None:
                if os.path.exists(self._path(key)) and os.path.getsize(self._path(key)) > self.limit:
                    raise MemoryError(f"disk cache limit exceeded by single key {key}")
                while self.size() > self.limit:
                    del self[self.lru()]
                gc.collect()

    def __delitem__(self, key):
        try:
            os.remove(self._path(key))
        except OSError:
            pass

    def __contains__(self, key):
        return os.path.isfile(self._path(key))

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
    
    def expired(self, key, ttl=None):
        with self._lock:
            if key not in self:
                return False
            if ttl is None:
                return False
            expired = int(time.time() - os.stat(self._path(key)).st_mtime) > ttl
            if expired:
                del self[key]
                return True
            return False

    def _path(self, key):
        return os.path.join(self.dir, key + self.EXTENSION)

    def _key(self, path):
        return os.path.basename(path)[0:-len(self.EXTENSION)]
