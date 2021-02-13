import os
from jardin import config as config
from jardin.cache_stores.disk import Disk

# Only use disk for now
config.init()
cache_config = config.CACHE_CONFIG
cache_dir = os.path.join(cache_config.get("dir", "/tmp"), 'jardin_cache')
cache_store = Disk(
    dir=cache_dir,
    limit=cache_config.get("limit", None))

def cached(func):
    global cached
    return _cached(func, cache_store)

def _cached(func, store):
    def wrapper(self, *args, **kwargs):
        cache = kwargs.pop('cache', False)
        if not cache:
            return func(self, *args, **kwargs)
        
        key = store.key(instance=self, caller=func, *args, **kwargs)
        if key not in store:
            store[key] = func(self, *args, **kwargs)
        return store[key]
    
    return wrapper