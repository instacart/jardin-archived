import logging

DATABASES = {
    'jardin_test': 'sqlite://localhost/jardin_test.db',
    # a db with multiple replica URLs. The 1st url works, but the 2nd is intentionally bogus.
    'multi_url_test': 'sqlite://localhost/jardin_test.db sqlite://localhost/bogus.db',
}

CACHE = {
    'method': 'disk',
    'options': {
        'dir': '/tmp/jardin_cache',
        'size': 10000
    }
}

LOG_LEVEL = logging.INFO
