import os
from jardin import config as config
from jardin.cache_stores.disk import Disk
from jardin.cache_stores.s3 import S3

config.init()

default_cache_method = config.CACHE.get("method", None)

STORES = {}
for method, cache_config in config.CACHE.get("methods", {}).items():
    klass = None
    if method == "disk":
        klass = Disk
    elif method == "s3":
        klass = S3
    if klass is not None:
        try:
            STORES[method] = klass(**cache_config)
        except Exception as ex:
            config.logging.warning(f"Could not initialize {method} cache.")
            config.logging.warning(ex)
                
def cached(func):
    
    def wrapper(self, *args, **kwargs):
        cache = kwargs.pop('cache', False)
        cache_method = kwargs.pop('cache_method', default_cache_method)
        store = STORES.get(cache_method, None)
        
        if (store is None or not store.valid) & (cache):
            config.logging.warning("Cache store is not correctly configured!")
            return func(self, *args, **kwargs)
        
        if not cache:
            return func(self, *args, **kwargs)
        
        ttl = kwargs.pop('ttl', None)
        key = store.key(instance=self, caller=func, *args, **kwargs)
        if key not in store or store.expired(key, ttl):
            store[key] = func(self, *args, **kwargs)
        return store[key]
    
    return wrapper
