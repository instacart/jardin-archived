import pandas

from jardin import config as config
from jardin.query_builders import \
    SelectQueryBuilder, \
    InsertQueryBuilder, \
    UpdateQueryBuilder, \
    DeleteQueryBuilder, \
    RawQueryBuilder
from jardin.cache_stores import cached

MAX_RETRIES = 3


def set_defaults(func):
    def wrapper(self, *args, **kwargs):
        kwargs.update(
            model_metadata=self.model_metadata,
            scheme=self.db_client.db_config.scheme,
            lexicon=self.db_client.lexicon
            )
        return func(self, *args, **kwargs)
    return wrapper


class DatabaseAdapter(object):

    def __init__(self, db_client_provider, model_metadata):
        self.db_client_provider = db_client_provider
        self.model_metadata = model_metadata

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
      while True:
        current_client = next(self.db_client_provider)
        if current_client is None:
          raise

        try:
          current_client.execute(*query, **kwargs)
        except current_client.connectivity_exceptions as e:
          current_client.ban()
