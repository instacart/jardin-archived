import logging
import os

os.environ["JARDIN_BAN_TIME_SECONDS"] = "3"
os.environ["JARDIN_MAX_RETRIES"] = "3"
os.environ["JARDIN_BACKOFF_BASE_TIME_SECONDS"] = "0"

PROTOCOL = "mysql"
PORT = int(os.environ.get("MYSQL_PORT", 3306))
USER = os.environ.get("MYSQ_USER", "root")
PASSWORD = os.environ.get("MYSQL_PWD", "")
DB   = os.environ.get("MYSQL_DB", "jardin_test")

base_connection = PROTOCOL + "://{user}:{password}@localhost:{port}/{db}"

DATABASES = {
    "jardin_test": base_connection.format(port=PORT, db=DB, user=USER, password=PASSWORD),

    "other_test_dict_config": {
        "username": "test",
        "password": "test",
        "database": "jardin_test",
        "host": "localhost",
        "port": 1234
    },

    # a db with multiple replica URLs. The 1st url refers to an active server. The 2nd url will fail to connect.
    "multi_url_test": f"{PROTOCOL}://{USER}:{PASSWORD}@localhost:{PORT}/{DB} {PROTOCOL}://{USER}:{PORT}@localhost:{PORT+1}/{DB}",

    "some_bad": " ".join([
        base_connection.format(port=PORT+0, user=USER, password=PASSWORD, db=DB),
        base_connection.format(port=PORT+1, user=USER, password=PASSWORD, db=DB + "_second"),
        base_connection.format(port=PORT+2, user=USER, password=PASSWORD, db=DB + "_thrid")
    ]),

    "all_bad": " ".join([
        base_connection.format(port=PORT+1, user=USER, password=PASSWORD, db=DB + "_first"),
        base_connection.format(port=PORT+2, user=USER, password=PASSWORD, db=DB + "_second"),
        base_connection.format(port=PORT+3, user=USER, password=PASSWORD, db=DB + "_thrid")
    ]),

    "all_good": " ".join([
        base_connection.format(port=PORT, user=USER, password=PASSWORD, db=DB),
        base_connection.format(port=PORT, user=USER, password=PASSWORD, db=DB),
        base_connection.format(port=PORT, user=USER, password=PASSWORD, db=DB)
    ]),

    "good_primary": base_connection.format(port=PORT+0, user=USER, password=PASSWORD, db=DB),

    "bad_primary":  base_connection.format(port=PORT+1, user=USER, password=PASSWORD, db=DB)
}

CACHE = {
    'method': 'disk',
    'options': {
        'dir': '/tmp/jardin_cache',
        'size': 10000
    }
}

LOG_LEVEL = logging.INFO
