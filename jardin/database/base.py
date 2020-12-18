from memoized_property import memoized_property
from contextlib import contextmanager
import threading


class BaseConnection(object):

    DRIVER = None
    LEXICON = None

    def __init__(self, db_config, name):
        self.db_config = db_config
        self.name = name
        self.lexicon = self.LEXICON()
        self._thread_local = threading.local()

    @contextmanager
    def connection(self):
        try:
            yield self.get_connection()
        except self.DRIVER.InterfaceError:
            self._thread_local.conn = None
            raise

    @memoized_property
    def connect_kwargs(self):
        return dict(
            database=self.db_config.database,
            user=self.db_config.username,
            password=self.db_config.password,
            host=self.db_config.host,
            port=self.db_config.port,
            connect_timeout=5
        )

    @memoized_property
    def connect_args(self):
        return []

    def get_connection(self):
        conn = getattr(self._thread_local, 'conn', None)
        if conn is None:
            conn = self.DRIVER.connect(*self.connect_args, **self.connect_kwargs)
            self._thread_local.conn = conn
        return conn

    @memoized_property
    def cursor_kwargs(self):
        return {}

    def columns(self, cursor):
        cursor_desc = cursor.description
        columns = []
        if cursor_desc:
            columns = [col_desc[0] for col_desc in cursor_desc]
            if self.db_config.lowercase_columns:
                columns = [col.lower() for col in columns]
        return columns

    def execute(self, *query, write=False, **kwargs):
        with self.connection() as connection:
            cursor = connection.cursor(**self.cursor_kwargs)
            cursor.execute(*query)
            if write:
                return self.lexicon.row_ids(cursor, kwargs['primary_key'])
            if cursor.description:
                return cursor.fetchall(), self.columns(cursor)
            return None, None