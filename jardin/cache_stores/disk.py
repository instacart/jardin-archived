import os
import pyarrow.feather as feather
import pandas as pd
from datetime import datetime

from jardin.cache_stores.base import Base

class Disk(Base):
    
    EXTENSION = '.feather'

    def __init__(self, dir, limit=None):
        self.dir = dir
        self.limit = limit
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
                raise MemoryError(f"disk cache limit exceeded by key {key}")
            while self.size() > self.limit:
                del self[self.lru()]

    def __delitem__(self, key):
        os.remove(self._path(key))

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

    def _path(self, key):
        return os.path.join(self.dir, key + self.EXTENSION)

    def _key(self, path):
        return os.path.basename(path)[0:-len(self.EXTENSION)]