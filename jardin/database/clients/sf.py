import re

import snowflake.connector as sf

from jardin.database.clients.pg import Lexicon as PGLexicon
from jardin.database.base_client import BaseClient


class Lexicon(PGLexicon):

    @staticmethod
    def extrapolator(_):
        return '%s'

    @staticmethod
    def format_args(args):
        return args.values()

    @staticmethod
    def standardize_interpolators(sql, params):
        sql, params = super(
            Lexicon, Lexicon).standardize_interpolators(sql, params)
        param_names = re.findall(r'\%\((\w+)\)s', sql)
        if len(param_names):
            sql = re.sub(r'\%\(\w+\)s', '%s', sql)
            if isinstance(params, dict):
                params = list(map(lambda x: params[x], param_names))
        return sql, params


class DatabaseClient(BaseClient):

    lexicon = Lexicon
    retryable_exceptions = (sf.InterfaceError, sf.OperationalError)

    def connect_impl(self):
        kwargs = dict(
            user=self.db_config.username,
            password=self.db_config.password,
            account=self.db_config.account,
            database=self.db_config.database,
            schema=self.db_config.schema,
            autocommit=True
        )
        if 'warehouse' in dir(self.db_config):
            kwargs['warehouse'] = self.db_config.warehouse
        if 'authenticator' in dir(self.db_config):
            kwargs['authenticator'] = self.db_config.authenticator
        if 'client_session_keep_alive' in dir(self.db_config):
            kwargs['client_session_keep_alive'] = self.db_config.client_session_keep_alive

        return sf.connect(**kwargs)

    def execute_impl(self, conn, *query):
        cursor = conn.cursor()
        cursor.execute(*query)
        return cursor
