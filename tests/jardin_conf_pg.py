import logging
import os

PGPORT = os.environ.get("PGPORT", 5432)

DATABASES = {
    'jardin_test': 'postgres://postgres:@localhost:%s/jardin_test' % PGPORT
}

LOG_LEVEL = logging.INFO
