import os
from jardin import config as config
from jardin.cache_stores.disk import Disk
from jardin.cache_stores.s3 import S3

config.init()

default_cache_method = config.CACHE.get("method", None)

STORES = {}
for method, cache_config in config.CACHE.get("methods", {}).values():
    if method == "disk":
        _store = Disk(dir=cache_config.get("dir", None),
                    limit=cache_config.get("limit", None))
        STORES[method] = _store
    elif method == "s3":
        bucket_name = cache_config.get("bucket_name", None)
        if bucket_name is not None:
            _store = S3(bucket_name=bucket_name,
                        path=cache_config.get("path", ""))
            STORES[method] = _store

def cached(func):
    
    def wrapper(self, *args, **kwargs):
        cache = kwargs.pop('cache', False)
        cache_method = kwargs.pop('cache_method', default_cache_method)
        store = STORES.get(cache_method, None)
        
        if store is None:
            if cache:
                config.logging.warning("Cache store is not configured!")
            return func(self, *args, **kwargs)
        
        if not cache:
            return func(self, *args, **kwargs)
        
        ttl = kwargs.pop('ttl', None)
        key = store.key(instance=self, caller=func, *args, **kwargs)
        if key not in store or store.expired(key, ttl):
            store[key] = func(self, *args, **kwargs)
        return store[key]
    
    return wrapper
