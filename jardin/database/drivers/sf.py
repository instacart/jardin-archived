import re
from memoized_property import memoized_property

import snowflake.connector as sf

from jardin.tools import retry
from jardin.database.drivers.pg import Lexicon as PGLexicon
from jardin.database.base import BaseConnection


class Lexicon(PGLexicon):

    @staticmethod
    def extrapolator(_):
        return '%s'

    @staticmethod
    def format_args(args):
        return args.values()

    @staticmethod
    def standardize_interpolators(sql, params):
        sql, params = super(Lexicon, Lexicon).standardize_interpolators(sql, params)
        param_names = re.findall(r'\%\((\w+)\)s', sql)
        if len(param_names):
            sql = re.sub(r'\%\(\w+\)s', '%s', sql)
            if isinstance(params, dict):
                params = list(map(lambda x: params[x], param_names))
        return sql, params


class DatabaseConnection(BaseConnection):

    DRIVER = sf
    LEXICON = Lexicon

    def __init__(self, db_config, name):
        super(DatabaseConnection, self).__init__(db_config, name)

    @memoized_property
    def connect_kwargs(self):
        kwargs = dict(
            user=self.db_config.username,
            password=self.db_config.password,
            account=self.db_config.account,
            database=self.db_config.database,
            schema=self.db_config.schema
            )
        if 'warehouse' in dir(self.db_config):
            kwargs['warehouse'] = self.db_config.warehouse
        return kwargs

    @retry(sf.OperationalError, tries=3)
    def connect(self):
        return super(DatabaseConnection, self).connect()

    @retry(sf.InterfaceError, tries=3)
    def execute(self, *query):
        return super(DatabaseConnection, self).execute(*query)
