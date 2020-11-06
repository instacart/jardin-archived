import imp, os, logging, sys

DEFAULTS = {'WATERMARK': '', 'LOG_LEVEL': logging.INFO}

INITIALIZED = False

def init():
    if INITIALIZED:
        return
    
    global DATABASES, CONNECTION_POOLS, WATERMARK, LOG_LEVEL, logger
    
    config_file = imp.load_source('jardin_conf', os.environ.get('JARDIN_CONF', 'jardin_conf.py'))
    
    DATABASES = config_file.DATABASES
    CONNECTION_POOLS = config_file.CONNECTION_POOLS if getattr(config_file, 'CONNECTION_POOLS', None) else {}
    
    this = sys.modules[__name__]
    
    for (k, v) in DEFAULTS.items():
        if hasattr(config_file, k):
            v = getattr(config_file, k)
        setattr(this, k, v)
    
    logging.basicConfig(level=LOG_LEVEL)
    logger = logging.getLogger('jardin')
    logger.setLevel(LOG_LEVEL)
    
    setattr(this, 'INITIALIZED', True)
