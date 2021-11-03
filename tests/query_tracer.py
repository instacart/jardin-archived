from jardin.database.base_client import BaseClient


class QueryTracer:
    @staticmethod
    def get_report():
        if not getattr(QueryTracer, "is_already_tracing", False):
            raise RuntimeError(f"get_report must be called within QueryTracer Context")
        return {
            "ban_list": QueryTracer.ban_list,
            "query_list": QueryTracer.query_list
        }

    def __enter__(self):
        if getattr(QueryTracer, "is_already_tracing", False):
            raise RuntimeError(f"Already tracing, cannot nest QueryTracer contexts")

        QueryTracer.ban_list = []
        QueryTracer.query_list = []
        QueryTracer.is_already_tracing = True
        QueryTracer.original_execute = BaseClient.execute
        QueryTracer.original_ban = BaseClient.ban

        BaseClient.execute = QueryTracer.traced_execute
        BaseClient.ban = QueryTracer.traced_ban

    def __exit__(self, exc_type, exc_value, exc_traceback):
        QueryTracer.is_already_tracing = False
        BaseClient.execute = QueryTracer.original_execute
        BaseClient.ban = QueryTracer.original_ban
        QueryTracer.query_list = []
        QueryTracer.ban_list = []

    @staticmethod
    def bound_method(instance, func):
        return func.__get__(instance, instance.__class__)

    @staticmethod
    def traced_ban(self, duration_seconds, exception):
        original_ban = QueryTracer.bound_method(self, QueryTracer.original_ban)
        QueryTracer.ban_list.append({
            "database_id": self.connection_identifier,
            "duration": duration_seconds
        })
        return original_ban(duration_seconds)

    @staticmethod
    def traced_execute(self, *query, **kwargs):
        original_execute = QueryTracer.bound_method(self, QueryTracer.original_execute)
        QueryTracer.query_list.append({
            "database_id": self.connection_identifier,
            "query": query,
            "kwargs": kwargs
        })
        return original_execute(*query, **kwargs)
