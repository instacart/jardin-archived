import logging
import os

PGPORT = os.environ.get("PGPORT", 5432)

DATABASES = {
    'jardin_test': 'postgres://postgres:@localhost:%s/jardin_test' % PGPORT,
    'other_test_dict_config': {
        'username': 'test',
        'password': 'test',
        'database': 'jardin_test',
        'host': 'localhost',
        'port': 1234
    }
}

LOG_LEVEL = logging.INFO
