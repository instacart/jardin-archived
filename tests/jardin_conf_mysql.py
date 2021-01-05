import logging

DATABASES = {
    'jardin_test': 'mysql://root:@localhost:3306/jardin_test',

    # a db with multiple replica URLs. The 1st url refers to an active server. The 2nd url will fail to connect.
    'multi_url_test': 'mysql://root:@localhost:3306/jardin_test mysql://root:@localhost:3333/jardin_test',
}

LOG_LEVEL = logging.INFO
