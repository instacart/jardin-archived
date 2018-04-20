import argparse
import os
import inspect

from jardin.database import DatabaseAdapter, DatabaseConnections
from jardin.tools import stack_marker

def query(sql=None, filename=None, extract=None, db=None, **kwargs):
    if db is None:
        raise argparse.ArgumentError('You must provide a database name')

    kwargs['stack'] = stack_marker(inspect.stack())

    filename = filename or extract

    if filename and not filename.startswith('/'):
        filename = os.path.join(os.environ['PWD'], filename)

    if 'where' not in kwargs and 'params' in kwargs:
        kwargs['where'] = kwargs['params']

    return DatabaseAdapter(
        DatabaseConnections.connection(db),
        None
        ).raw_query(
            sql=sql, filename=filename, **kwargs
        )
