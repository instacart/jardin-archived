from future.standard_library import install_aliases
install_aliases()
from urllib.parse import urlparse


class UnknownConfigFormat(Exception): pass


class DatabaseConfig(object):

    lowercase_columns = False

    def __init__(self, config):
        if isinstance(config, str) or isinstance(config, unicode):
            db = urlparse(config)
            self.scheme = db.scheme
            self.username = db.username
            self.password = db.password
            self.host = db.hostname
            self.port = db.port
            self.database = db.path[1:]
        elif isinstance(config, dict):
            for (k, v) in config.items():
                setattr(self, k, v)
        else:
            raise UnknownConfigFormat(type(config), config)