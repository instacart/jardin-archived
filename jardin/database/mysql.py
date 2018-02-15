import MySQLdb

from jardin.tools import retry


class DatabaseConnection(BaseConnection):

    DRIVER = MySQLdb

    @retry(MySQLdb.OperationalError, tries=3)
    def connect(self):
        return super(DatabaseConnection, self).connect()

    @retry(MySQLdb.InterfaceError, tries=3)
    def execute(self, *query):
        return super(DatabaseConnection, self).execute(*query)
