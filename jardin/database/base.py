from memoized_property import memoized_property
from contextlib import contextmanager
import threading


class BaseConnection(object):

    DRIVER = None
    LEXICON = None

    _connection = None

    def __init__(self, db_config, name, pool_config=None):
        self.db_config = db_config
        self.autocommit = True
        self.name = name
        self.lexicon = self.LEXICON()
        self.pool_config = pool_config

    @contextmanager
    def connection(self):
        try:
            conn = self.get_connection()
            yield conn
            if self.autocommit:
                conn.commit()
        except Exception as e:
            self.rollback()
        finally:
            if self.pool is not None and self.autocommit:
                key = threading.current_thread().ident
                self.pool.putconn(conn, key=key)

    def commit(self):
        conn = self.get_connection()
        conn.commit()
        if self.pool:
            key = threading.current_thread().ident
            self.pool.putconn(conn, key=key)

    def rollback(self):
        conn = self.get_connection()
        conn.rollback()

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

    @memoized_property
    def pool(self):
        return None

    def get_connection(self):
        if self.pool is None:
            if self._connection is None:
                self._connection = self.DRIVER.connect(*self.connect_args, **self.connect_kwargs)
            return self._connection
        key = threading.current_thread().ident
        return self.pool.getconn(key=key)

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