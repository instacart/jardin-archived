import logging
import os

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = int(os.environ.get("PGPORT", 5432))

DATABASES = {
    'jardin_test': f'postgres://postgres:@{PGHOST}:{PGPORT}/jardin_test',
    'other_test_dict_config': {
        'username': 'test',
        'password': 'test',
        'database': 'jardin_test',
        'host': PGHOST,
        'port': 1234
    },

    # a db with multiple replica URLs. The 1st url refers to an active server. The 2nd url will fail to connect.
    'multi_url_test': f'postgres://postgres:@localhost:{PGPORT}/jardin_test postgres://postgres:@localhost:{PGPORT+1}/jardin_test',
}

LOG_LEVEL = logging.INFO
