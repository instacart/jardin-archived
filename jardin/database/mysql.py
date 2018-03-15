import sys
if sys.version_info[0] < 3:
    import MySQLdb
else:
    import pymysql as MySQLdb

from jardin.tools import retry
from jardin.database.base import BaseConnection


class DatabaseConnection(BaseConnection):

    DRIVER = MySQLdb

    @retry(DRIVER.OperationalError, tries=3)
    def connect(self):
        return super(DatabaseConnection, self).connect()

    @retry(DRIVER.InterfaceError, tries=3)
    def execute(self, *query):
        return super(DatabaseConnection, self).execute(*query)

    @staticmethod
    def table_schema_query(table_name):
        return "SHOW COLUMNS FROM %s;" % table_name