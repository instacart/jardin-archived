from memoized_property import memoized_property
from contextlib import contextmanager
import threading


class BaseConnection(object):

    DRIVER = None
    LEXICON = None

    _cache = threading.local()

    def __init__(self, db_config, name):
        self.db_config = db_config
        self.autocommit = True
        self.name = name
        self.lexicon = self.LEXICON()

    @contextmanager
    def connection(self):
        try:
            conn = self.get_connection()
            yield conn
            if self.autocommit:
                conn.commit()
        except self.DRIVER.InterfaceError:
            # TODO [kl] verify that 'conn' on '_cache' will always exist
            self._cache.conn = None
            raise
        except Exception as e:
            # TODO [kl] old code has potential infinite loop b/c rollback() calls back to this function
            self.rollback()
            # TODO [kl] old code was not raising here, but that seemed wrong to swallow the exception
            raise

    def commit(self):
        self.get_connection().commit()

    def rollback(self):
        self.get_connection().rollback()

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
        # TODO [kl] cleanup the does 'conn' property exist-or-not junk (also see TODO on line 27)
        conn = self._cache.conn if hasattr(self._cache, 'conn') else None
        if conn is None:
            conn = self.DRIVER.connect(*self.connect_args, **self.connect_kwargs)
            self._cache.conn = conn
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