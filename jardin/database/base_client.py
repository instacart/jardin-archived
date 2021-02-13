import time
from abc import ABC, abstractmethod

MAX_RETRIES = 3


class BaseClient(ABC):

    def __init__(self, db_config, name):
        self.db_config = db_config
        self.name = name
        self._conn = None
        
    @property
    def default_connect_kwargs(self):
        return dict(
            database=self.db_config.database,
            user=self.db_config.username,
            password=self.db_config.password,
            host=self.db_config.host,
            port=self.db_config.port,
            connect_timeout=5
        )

    @property
    @abstractmethod
    def lexicon(self):
        """Provide an object which normalizes a SQL dialect."""\

    @property
    @abstractmethod
    def retryable_exceptions(self):
        """Provide exceptions which may be retried."""

    @abstractmethod
    def connect_impl(self):
        """Connect to a SQL database."""

    @abstractmethod
    def execute_impl(self, conn, *query):
        """Execute a SQL query and return the cursor."""

    def execute(self, *query, **kwargs):
        last_exception = None
        delay = 3
        for _ in range(MAX_RETRIES):
            try:
                return self.execute_once(*query, **kwargs)
            except self.retryable_exceptions as e:
                # TODO [kl] comment this out?
                print(f"{e}, Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
                last_exception = e
                continue
        else:
            raise last_exception

    def execute_once(self, *query, write=False, **kwargs):
        """Connect to the database (if necessary) and execute a query."""
        if self._conn is None:
            self._conn = self.connect_impl()

        try:
            cursor = self.execute_impl(self._conn, *query)
        except self._conn.InterfaceError:
            # the connection is probably closed; stop re-using the connection
            self._conn = None
            raise

        if write:
            return self.lexicon.row_ids(cursor, kwargs['primary_key'])
        if cursor.description:
            return cursor.fetchall(), self.columns(cursor)
        return None, None

    def columns(self, cursor):
        cursor_desc = cursor.description
        columns = []
        if cursor_desc:
            columns = [col_desc[0] for col_desc in cursor_desc]
            if self.db_config.lowercase_columns:
                columns = [col.lower() for col in columns]
        return columns