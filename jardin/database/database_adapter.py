import pandas
import time
import os

from jardin import config as config
from jardin.query_builders import \
    SelectQueryBuilder, \
    InsertQueryBuilder, \
    UpdateQueryBuilder, \
    DeleteQueryBuilder, \
    RawQueryBuilder
from jardin.cache_stores import cached

def set_defaults(func):
    def wrapper(self, *args, **kwargs):
        kwargs.update(
            model_metadata=self.model_metadata,
            scheme=self.client_provider.config().scheme,
            lexicon=self.client_provider.lexicon()
            )
        return func(self, *args, **kwargs)
    return wrapper


class DatabaseAdapter(object):
    def __init__(self, client_provider, model_metadata):
        self.client_provider = client_provider
        self.model_metadata = model_metadata

    @classmethod
    def backoff_base_time(self):
        if getattr(self, "_backoff_base_time", None) is None:
           self._backoff_base_time = int(os.environ.get("JARDIN_BACKOFF_BASE_TIME_SECONDS", 3))
        return self._backoff_base_time

    @classmethod
    def max_retries(self):
        if getattr(self, "_max_retries", None) is None:
            self._max_retries = int(os.environ.get("JARDIN_MAX_RETRIES", 3))
        return self._max_retries

    @classmethod
    def ban_time(self):
        if getattr(self, "_ban_time", None) is None:
            self._ban_time = int(os.environ.get("JARDIN_BAN_TIME_SECONDS", 1))
        return self._ban_time

    @set_defaults
    def select(self, **kwargs):
        query = SelectQueryBuilder(**kwargs).query
        config.logger.debug(query)
        results, columns = self._execute(*query, write=False)
        if results is None and columns is None:
            return None
        return pandas.DataFrame.from_records(results, columns=columns, coerce_float=True)

    @set_defaults
    def write(self, query_builder, **kwargs):
        query = query_builder(**kwargs).query
        config.logger.debug(query)
        returning_ids = self._execute(*query, write=True, **kwargs)
        if len(returning_ids) > 0:
            return self.select(where={kwargs['primary_key']: returning_ids})
        return None

    def insert(self, **kwargs):
        return self.write(InsertQueryBuilder, **kwargs)

    def update(self, **kwargs):
        return self.write(UpdateQueryBuilder, **kwargs)

    @set_defaults
    def delete(self, **kwargs):
        query = DeleteQueryBuilder(**kwargs).query
        config.logger.debug(query)
        self._execute(*query, write=False)

    @set_defaults
    @cached
    def raw_query(self, **kwargs):
        query = RawQueryBuilder(**kwargs).query
        config.logger.debug(query)
        results, columns = self._execute(*query, write=False)
        if results is None and columns is None:
            return None
        return pandas.DataFrame.from_records(results, columns=columns, coerce_float=True)

    def _execute(self, *query, **kwargs):
      last_exception = None
      while True:
        current_client = self.client_provider.next_client()
        if current_client is None:
            raise last_exception

        backoff = self.__class__.backoff_base_time()
        for _ in range(self.__class__.max_retries()):
          try:
              return current_client.execute(*query, **kwargs)
          except current_client.retryable_exceptions as e:
              time.sleep(backoff)
              backoff *= 2
              last_exception = e
              continue
        else:
          if last_exception.__class__ in current_client.connectivity_exceptions:
              current_client.ban(self.__class__.ban_time()) # ban connection for a few seconds and try again with a different connection
