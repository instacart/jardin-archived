jardin
======

Jardin is a ``pandas.DataFrame``-based ORM for Python applications.

Getting started
---------------

In your working directory (the root of your app), create a file named
``jardin_conf.py``:

.. code:: python

    # jardin_conf.py

    DATABASES = {
      'my_first_database': 'https://username:password@database.url:port',
      'my_second_database': 'https://username:password@database.url:port'
    }

    LOG_LEVEL = logging.DEBUG

    WATERMARK = 'My Great App'

Then, in your app, say you have a table called ``users``:

.. code:: python

    # app.py
    import jardin

    class User(jardin.Model):
      db_names = {'read': 'my_first_database', 'write': 'my_second_database'}

In the console:

.. code:: python

    >>> from app import Users
    >>> users = User.last(4)
    # /* My Great App */ SELECT * FROM users ORDER BY u.created_at DESC LIMIT 4;
    >>> users
    id   name    email              ...
    0    John    john@beatl.es      ...
    1    Paul    paul@beatl.es      ...
    2    George  george@beatl.es    ...
    3    Ringo   ringo@beatl.es     ...

The resulting object is a pandas dataframe:

.. code:: python

    >>> import pandas
    >>> isinstance(users, pandas.DataFrame)
    True
    >>> isinstance(users, jardin.Collection)
    True

Queries
-------

SELECT queries
~~~~~~~~~~~~~~

Here is the basic syntax to select records from the database

.. code:: python

    >>> users = User.select(select = ['id', 'name'], where = {'email': 'paul@beatl.es'},
                             order = 'id ASC', limit = 1)
    # /* My Great App */ SELECT u.id, u.name FROM users u WHERE u.email = 'paul@beatl.es' ORDER BY u.id ASC LIMIT 1;
    >>> users
    id   name
    1    Paul

Arguments
^^^^^^^^^

-  ``select`` – The list of columns to return. If not provided, all
   columns will be returned.
-  ``where`` – conditions. Many different formats can be used to provide
   conditions. See `docs <#where-argument>`__.
-  ``inner_join``, ``left_join`` – List of tables to join with their
   join condition. Can also be a list of classes if the appropriate
   associations have been declared. See
   `docs <#inner_join-left_join-arguments>`__.
-  ``order`` – order clause
-  ``limit`` – limit clause
-  ``group`` – grouping clause
-  ``scopes`` – list of pre-defined scopes. See docs.

``where`` argument
''''''''''''''''''

Here are the different ways to feed a condition clause to a query. \*
``where = "name = 'John'"`` \* ``where = {'name': 'John'}`` \*
``where = {'id': (0, 3)}`` – selects where ``id`` is between 0 and 3 \*
``where = {'id': [0, 1, 2]}`` – selects where ``id`` is in the array \*
``where = [{'id': (0, 10), 'instrument': 'drums'}, ["created_at > %(created_at)s", {'created_at': '1963-03-22'}]]``

``inner_join``, ``left_join`` arguments
'''''''''''''''''''''''''''''''''''''''

The simplest way to join another table is as follows

.. code:: python

    >>> User.select(inner_join = ["instruments i ON i.id = u.instrument_id"])

If you have configured your models associations, see
`here <#associations>`__, you can simply pass the class as argument:

.. code:: python

    >>> User.select(inner_join = [Instruments])

Individual record selection
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can also look-up a single record by id:

.. code:: python

    >>> User.find(1)
    # /* My Great App */ SELECT * FROM users u WHERE u.id = 1;
    {'id': 1, 'name': 'Paul', 'email': 'paul@beatl.es', ...}

Note that the returned object is a ``Record`` object which allows you to
access attributes in those way:

.. code:: python

    >>> user['name']
    Paul
    >>> user.name
    Paul

INSERT queries
~~~~~~~~~~~~~~

.. code:: python

    >>> user = User.insert(name = 'Pete', email = 'pete@beatl.es')
    # /* My Great App */ INSERT INTO users (name, email) VALUES ('Pete', 'pete@beatl.es') RETURNING id;
    # /* My Great App */ SELECT u.* FROM users WHERE u.id = 4;
    >>> user
    id   name    email
    4    Pete    pete@beatl.es

UPDATE queries
~~~~~~~~~~~~~~

.. code:: python

    >>> users = User.update(values = {'hair': 'long'}, where = {'name': 'John'})
    # /* My Great App */ UPDATE users u SET (u.hair) = ('long') WHERE u.name = 'John' RETURNING id;
    # /* My Great App */ SELECT * FROM users u WHERE u.name = 'John';

DELETE queries
~~~~~~~~~~~~~~

.. code:: python

    >>> User.delete(where = {'id': 1})
    # /* My Great App */ DELETE FROM users u WHERE u.id = 1;

Associations
------------

It is possible to define associations between models. For example, if
each user has multiple instruments:

.. code:: python

    # app.py

    import jardin

    class MyModel(jardin.Model):
      db_names = {'read': 'my_first_database', 'write': 'my_second_database'}

    class Instrument(MyModel):
      belongs_to = {'users': 'user_id'}

    class User(MyModel):
      has_many = [Instruments]

and then you can query the associated records:

.. code:: python

    >>> users = User.select()
    # /* My Great App */ SELECT * FROM users u;
    >>> instruments = users.instruments()
    # /* My Great App */ SELECT * FROM instruments i WHERE i.id IN (0, 1, ...);

Or you can declare joins more easily

.. code:: python

    >>> users = User.select(inner_join = [Instruments])

Scopes
------

Queries conditions can be generalized across your app:

.. code:: python

    # app.py

    class User(jardin.Model):
      scopes = {
        'alive': {'name': ['Paul', 'Ringo']},
        'guitarists': {'name': ['John', 'George']}
      }

The key is the name of the scope, and the value is the conditions to be
applied. Anything that can be fed to the ``where`` argument of
``Model#select`` can be used to define a scope.

Use them as such:

.. code:: python

    >>> users = User.select(scopes = ['alive'], ...)
    # /* My Great App */ SELECT * FROM users u WHERE u.name IN ('Paul', 'Ringo') AND ...;

Misc
----

Watermark and trace
~~~~~~~~~~~~~~~~~~~

Multiple databases
~~~~~~~~~~~~~~~~~~
