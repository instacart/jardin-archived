from urllib.parse import urlparse


class UnknownConfigFormat(Exception): pass


class DatabaseConfig(object):

    lowercase_columns = False

    def __init__(self, config, db_name):
        if not config:
            raise RuntimeError(f"Missing config url for DB '{db_name}'")

        if isinstance(config, str):
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
            raise UnknownConfigFormat(type(config), config, f"db='{db_name}'")
