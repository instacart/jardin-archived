import logging
import os
os.environ["JARDIN_BAN_TIME_SECONDS"] = "3"
os.environ["JARDIN_MAX_RETRIES"] = "3"
os.environ["JARDIN_BACKOFF_BASE_TIME_SECONDS"] = "0"

PGPORT = int(os.environ.get("PGPORT", 5432))
PGUSER = os.environ.get("PGUSER", 'postgres')
PGPASS = os.environ.get("PGPASSWORD", '')
PGDB   = os.environ.get("PGDATABASE", 'jardin_test')

base_connection = "postgres://{user}:{password}@localhost:{port}/{db}"
DATABASES = {
    'jardin_test': base_connection.format(port=PGPORT, db=PGDB, user=PGUSER, password=PGPASS),
    'other_test_dict_config': {
        'username': 'test',
        'password': 'test',
        'database': 'jardin_test',
        'host': 'localhost',
        'port': 1234
    },

    # a db with multiple replica URLs. The 1st url refers to an active server. The 2nd url will fail to connect.
    'multi_url_test': f'postgres://{PGUSER}:{PGPASS}@localhost:{PGPORT}/{PGDB} postgres://{PGUSER}:{PGPORT}@localhost:{PGPORT+1}/{PGDB}',

    'some_bad': " ".join([
        base_connection.format(port=PGPORT+0, user=PGUSER, password=PGPASS, db=PGDB),
        base_connection.format(port=PGPORT+1, user=PGUSER, password=PGPASS, db=PGDB + "_second"),
        base_connection.format(port=PGPORT+2, user=PGUSER, password=PGPASS, db=PGDB + "_thrid")
    ]),

    'all_bad': " ".join([
        base_connection.format(port=PGPORT+1, user=PGUSER, password=PGPASS, db=PGDB + "_first"),
        base_connection.format(port=PGPORT+2, user=PGUSER, password=PGPASS, db=PGDB + "_second"),
        base_connection.format(port=PGPORT+3, user=PGUSER, password=PGPASS, db=PGDB + "_thrid")
    ]),

    'all_good': " ".join([
        base_connection.format(port=PGPORT, user=PGUSER, password=PGPASS, db=PGDB),
        base_connection.format(port=PGPORT, user=PGUSER, password=PGPASS, db=PGDB),
        base_connection.format(port=PGPORT, user=PGUSER, password=PGPASS, db=PGDB)
    ]),

    'good_primary': base_connection.format(port=PGPORT+0, user=PGUSER, password=PGPASS, db=PGDB),
    'bad_primary':  base_connection.format(port=PGPORT+1, user=PGUSER, password=PGPASS, db=PGDB)
}

LOG_LEVEL = logging.INFO
