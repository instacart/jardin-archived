from memoized_property import memoized_property
from contextlib import contextmanager

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
            if self._connection is None:
                self._connection = self.connect()
            yield self._connection
        except self.DRIVER.InterfaceError:
            self._connection = None
            raise
        except BaseConnection as e:
            connection.rollback()
            raise
        finally:
            if self.pool is not None:
                self.pool.putconn(self._connection)
                self._connection = None
                
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

    def connect(self):
        if self.pool is not None:
            return self.pool.getconn()
        return self.DRIVER.connect(*self.connect_args, **self.connect_kwargs)

    @memoized_property
    def cursor_kwargs(self):
        return {}
        
    def execute(self, *query):
        with self.connection() as connection:
            cursor = connection.cursor(**self.cursor_kwargs)
            cursor.execute(*query)
            return cursor.fetchall(), self.columns(cursor)

    def columns(self, cursor):
        cursor_desc = cursor.description
        columns = []
        if cursor_desc:
            columns = [col_desc[0] for col_desc in cursor_desc]
            if self.db_config.lowercase_columns:
                columns = [col.lower() for col in columns]
        return columns