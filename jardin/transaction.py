from jardin.database import DatabaseConnections
from jardin.query import query

class Transaction(object):

    def __init__(self, db):
        self.connection = DatabaseConnections.connection(db)

    def __enter__(self):
        self.connection.autocommit = False
        query(
            sql=self.connection.lexicon.transaction_begin_query(),
            db=self.connection.name
            )

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.connection.connection().commit()
        else:
            self.connection.connection().rollback()
        self.connection.autocommit = True
