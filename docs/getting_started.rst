Getting started
===============

Installation
------------

.. code-block:: shell

  $ pip install jardin

or

.. code-block:: shell

  $ echo 'jardin' >> requirements.txt
  $ pip install -r requirements.txt

Setup
-----

In your working directory (the root of your app), create a file named ``jardin_conf.py``::


  # jardin_conf.py

  DATABASES = {
    'my_master_database': 'https://username:password@master_database.url:port',
    'my_replica_database': 'https://username:password@replica_database.url:port'
  }

  LOG_LEVEL = logging.DEBUG

  WATERMARK = 'My Great App'

You can also place this file anywhere you want and point to it with the environment variable ``JARDIN_CONF``.

Then, in your app, say you have a table called ``users``::


  # app.py
  import jardin

  class User(jardin.Model):
    db_names = {'master': 'my_master_database', 'replica': 'my_replica_database'}

In the console::

  >>> from app import User
  >>> users = User.last(4)
  # /* My Great App */ SELECT * FROM users ORDER BY u.created_at DESC LIMIT 4;
  >>> users
  id   name    email              ...
  0    John    john@beatl.es      ...
  1    Paul    paul@beatl.es      ...
  2    George  george@beatl.es    ...
  3    Ringo   ringo@beatl.es     ...

The resulting object is a pandas dataframe::

  >>> import pandas
  >>> isinstance(users, pandas.DataFrame)
  True
  >>> isinstance(users, jardin.Collection)
  True
