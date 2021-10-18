import logging
import os

MYSQL_PWD = os.environ.get("MYSQL_PWD", "")

DATABASES = {
    'jardin_test': f'mysql://root:{MYSQL_PWD}@localhost:3306/jardin_test',

    # a db with multiple replica URLs. The 1st url refers to an active server. The 2nd url will fail to connect.
    'multi_url_test': f'mysql://root:{MYSQL_PWD}@localhost:3306/jardin_test mysql://root:{MYSQL_PWD}@localhost:3333/jardin_test',
}

CACHE = {
    'method': 'disk',
    'options': {
        'dir': '/tmp/jardin_cache',
        'size': 10000
    }
}

LOG_LEVEL = logging.INFO
