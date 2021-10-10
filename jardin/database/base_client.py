import time
import sys
from abc import ABC, abstractmethod

class BaseClient(ABC):

    def __init__(self, db_config, name):
        self.db_config = db_config
        self.name = name
        self._conn = None
        self._banned_until = None
        self.id = ":".join([self.db_config.host, self.db_config.database])

    def connection_identifier(self):
      return self.id

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

    @property
    @abstractmethod
    def connectivity_exceptions(self):
        """Provide exceptions which should trigger temporary connection banning."""


    @abstractmethod
    def connect_impl(self):
        """Connect to a SQL database."""

    @abstractmethod
    def execute_impl(self, conn, *query):
        """Execute a SQL query and return the cursor."""

    def unban(self):
      self._banned_until = None

    def ban(self, seconds=1):
      self._banned_until = time.time() + seconds
      self.safely_disconnect()

    def is_banned(self):
      if self._banned_until is None:
        return False

      if self._banned_until < time.time():
        return False

      return True

    def execute(self, *query, write=False, **kwargs):
        """Connect to the database (if necessary) and execute a query."""
        cursor = None
        try:
            if self._conn is None:
              self._conn = self.connect_impl()
            cursor = self.execute_impl(self._conn, *query)
        except self.connectivity_exceptions as e:
            self.safely_disconnect()
            raise

        if write:
            return self.lexicon.row_ids(cursor, kwargs['primary_key'])
        if cursor.description:
            return cursor.fetchall(), self.columns(cursor)
        return None, None

    def safely_disconnect(self):
      try:
          # this assumes all implementations have a close method
          if self._conn is not None:
              self._conn.close()
      except AttributeError:
        raise # _conn does not have .close() method, let's raise
      except:
          # Failing to close a connection should be okay
          pass
      finally:
          # This will prompt execute to reconnect the next time it is called
          self._conn = None

    def columns(self, cursor):
        cursor_desc = cursor.description
        columns = []
        if cursor_desc:
            columns = [col_desc[0] for col_desc in cursor_desc]
            if self.db_config.lowercase_columns:
                columns = [col.lower() for col in columns]
        return columns
