import imp, os, logging, sys

from jardin.instrumentation.notifier import Notifier

DEFAULTS = {
    'APPLICATION_NAME': None,
    'WATERMARK': '',
    'LOG_LEVEL': logging.INFO,
    'CACHE': {
        'method': None,
        'methods': {
            's3': {
                'bucket_name': None,
                'path': ''
            },
            'disk': {
                'dir': None,
                'limit': None
            }
        }
    }
}

INITIALIZED = False

def init():
    if INITIALIZED:
        return

    global APPLICATION_NAME, DATABASES, CACHE, WATERMARK, LOG_LEVEL, logger, notifier

    config_file = imp.load_source('jardin_conf', os.environ.get('JARDIN_CONF', 'jardin_conf.py'))

    DATABASES = config_file.DATABASES

    this = sys.modules[__name__]

    for (k, v) in DEFAULTS.items():
        if hasattr(config_file, k):
            v = getattr(config_file, k)
        setattr(this, k, v)

    logging.basicConfig(level=LOG_LEVEL)
    logger = logging.getLogger('jardin')
    logger.setLevel(LOG_LEVEL)

    notifier = Notifier()

    setattr(this, 'INITIALIZED', True)
