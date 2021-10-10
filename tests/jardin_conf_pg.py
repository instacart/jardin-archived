import logging
import os

PGPORT = int(os.environ.get("PGPORT", 5432))
base_connection = "postgres://main_user:@localhost:{port}/{db}"
DATABASES = {
    'jardin_test': base_connection.format(port=PGPORT, db="main_db"),
    'other_test_dict_config': {
        'username': 'main_user',
        'password': '',
        'database': 'main_db',
        'host': 'localhost',
        'port': 1234
    },

    # a db with multiple replica URLs. The 1st url refers to an active server. The 2nd url will fail to connect.
    'multi_url_test': f'postgres://main_user:@localhost:{PGPORT}/main_db postgres://main_user:@localhost:{PGPORT+1}/main_db',

    'some_bad': " ".join([
      base_connection.format(port=PGPORT+0, db="main_db"),
      base_connection.format(port=PGPORT+1, db="second"),
      base_connection.format(port=PGPORT+2, db="third"),
    ]),

    'all_bad': " ".join([
      base_connection.format(port=PGPORT+1, db="first"),
      base_connection.format(port=PGPORT+2, db="second"),
      base_connection.format(port=PGPORT+3, db="third"),
    ]),

    'all_good': " ".join([
      base_connection.format(port=PGPORT, db="main_db"),
      base_connection.format(port=PGPORT, db="main_db"),
      base_connection.format(port=PGPORT, db="main_db"),
    ]),

    'good_primary': base_connection.format(port=PGPORT+0, db="main_db"),
    'bad_primary':  base_connection.format(port=PGPORT+1, db="main_db"),
}

LOG_LEVEL = logging.INFO
