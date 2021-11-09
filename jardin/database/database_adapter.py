import pandas
import time
import os

from jardin import config as config
from jardin.instrumentation.event import Event, EventExceptionInformation
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
            scheme=self.client_provider.config.scheme,
            lexicon=self.client_provider.lexicon
        )
        return func(self, *args, **kwargs)
    return wrapper


class NoAvailableConnectionsError(RuntimeError):
    def __init__(self, datasource_name):
        super().__init__("NoAvailableConnections in {} data source".format(datasource_name))

class DatabaseAdapter(object):

    backoff_base_time = int(os.environ.get("JARDIN_BACKOFF_BASE_TIME_SECONDS", 3))
    max_retries       = int(os.environ.get("JARDIN_MAX_RETRIES", 3))
    ban_time          = int(os.environ.get("JARDIN_BAN_TIME_SECONDS", 1))

    def __init__(self, client_provider, model_metadata):
        self.client_provider = client_provider
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
        last_exception = None
        while True:
            current_client = self.client_provider.next_client()
            if current_client is None:
                exception_info = EventExceptionInformation(last_exception) if last_exception is not None else None
                config.notifier.report_event(Event("no_available_connections_raised", error=exception_info, tags={"db_name": self.client_provider.name}))
                raise NoAvailableConnectionsError(self.client_provider.datasource_name) from last_exception

            backoff = self.backoff_base_time
            for attempt_no in range(self.max_retries):
                if attempt_no > 0:
                    config.notifier.report_event(Event("query_retry", error=EventExceptionInformation(last_exception), tags=current_client.tags()))

                try:
                    return current_client.execute(*query, **kwargs)
                except current_client.retryable_exceptions as e:
                    time.sleep(backoff)
                    backoff *= 2
                    last_exception = e
                    continue
            else:
                if issubclass(type(last_exception), current_client.connectivity_exceptions):
                    # ban connection for a few seconds and try again with a different connection
                    current_client.ban(self.ban_time, last_exception)
