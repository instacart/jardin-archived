from abc import ABCMeta, abstractmethod

import hashlib
import inspect


class Base(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

    @abstractmethod
    def __delitem__(self, key):
        pass

    @abstractmethod
    def __contains__(self, key):
        pass

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def keys(self):
        pass

    @abstractmethod
    def values(self):
        pass
    
    @abstractmethod
    def expired(self, key, ttl=None):
        pass
    
    @property
    @abstractmethod
    def valid(self):
        return True

    def key(self, *args, **kwargs):
        stack = inspect.stack()
        caller = kwargs.pop('caller', stack[-2])
        instance = kwargs.pop('instance', self)
        kwargs.pop('stack', None)

        return '.'.join((
            instance.__module__,
            instance.__class__.__name__,
            hashlib.sha1(str(args).encode('utf-8') + str(kwargs).encode('utf-8')).hexdigest()
        ))
        
    def clear(self):
        for key in self.keys():
            del self[key]

