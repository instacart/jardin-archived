import imp
import logging
import os
import sys

DEFAULTS = {'WATERMARK': '', 'LOG_LEVEL': logging.INFO}

INITIALIZED = False

def load_config_file():
    settings_file = os.environ.get('JARDIN_CONF', 'jardin_conf.py')    
    return imp.load_source('jardin_conf', settings_file)

def init():
    if INITIALIZED:
        return

    global DATABASES, WATERMARK, LOG_LEVEL, logger

    config_file = load_config_file()
    
    DATABASES = config_file.DATABASES
    this = sys.modules[__name__]

    for k, v in DEFAULTS.items():
        if hasattr(config_file, k):
            v = getattr(config_file, k)
        setattr(this, k, v)

    logging.basicConfig(level=LOG_LEVEL)
    logger = logging.getLogger('jardin')
    logger.setLevel(LOG_LEVEL)

    setattr(this, 'INITIALIZED', True)
