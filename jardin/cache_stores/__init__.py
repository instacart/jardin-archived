import os
from jardin import config as config
from jardin.cache_stores.disk import Disk

config.init()

cache_config = config.CACHE
cahe_options = cache_config.get("options", {})
cache_dir = os.path.join(cahe_options.get("dir", "/tmp"), 'jardin_cache')

cache_store = Disk(dir=cache_dir,limit=cahe_options.get("limit", None))
    

def cached(func):
    global cached
    return _cached(func, cache_store)

def _cached(func, store):
    def wrapper(self, *args, **kwargs):
        cache = kwargs.pop('cache', False)
        if not cache:
            return func(self, *args, **kwargs)
        
        ttl = kwargs.pop('ttl', None)
        key = store.key(instance=self, caller=func, *args, **kwargs)
        if key not in store or store.expired(key, ttl):
            store[key] = func(self, *args, **kwargs)
        return store[key]
    
    return wrapper