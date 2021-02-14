import os
from jardin import config as config
from jardin.cache_stores.disk import Disk

config.init()

cache_method = config.CACHE.get("method", None)

if cache_method == "disk":
    cahe_options = config.CACHE.get("options", {})
    cache_store = Disk(dir=cahe_options.get("dir", "/tmp/jardin_cache"),
                    limit=cahe_options.get("limit", None))
else:
    cache_store = None
    

def cached(func):
    global cached
    return _cached(func, cache_store)

def _cached(func, store):
    def wrapper(self, *args, **kwargs):
        if store is None:
            return func(self, *args, **kwargs)
        
        cache = kwargs.pop('cache', False)
        if not cache:
            return func(self, *args, **kwargs)
        
        ttl = kwargs.pop('ttl', None)
        key = store.key(instance=self, caller=func, *args, **kwargs)
        if key not in store or store.expired(key, ttl):
            store[key] = func(self, *args, **kwargs)
        return store[key]
    
    return wrapper
