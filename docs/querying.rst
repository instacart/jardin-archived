Querying
========

SELECT queries
--------------

Here is the basic syntax to select records from the database

  >>> users = User.select(
                select=['id', 'name'],
                where={'email': 'paul@beatl.es'},
                order='id ASC',
                limit=1)
  # SELECT u.id, u.name FROM users u WHERE u.email = 'paul@beatl.es' ORDER BY u.id ASC LIMIT 1; /* My Great App */ 
  >>> users
  id   name
  1    Paul


Arguments
~~~~~~~~~

See :doc:`api_reference`.

**where argument**

Here are the different ways to feed a condition clause to a query.

  * ``where = "name = 'John'"``
  * ``where = {'name': 'John'}``
  * ``where = {'id': (0, 3)}`` – selects where ``id`` is between 0 and 3
  * ``where = {'id': [0, 1, 2]}`` – selects where ``id`` is in the array
  * ``where = [{'id': (0, 10), 'instrument': 'drums'}, ["created_at > %(created_at)s", {'created_at': '1963-03-22'}]]``

For other operators than ``=``, see :doc:`comparators`.

**inner_join, left_join arguments**

The simplest way to join another table is as follows

  >>> User.select(inner_join=["instruments i ON i.id = u.instrument_id"])

If you have configured your models associations, see :doc:`features`, you can simply pass the class as argument::

  >>> User.select(inner_join=[Instrument])

Individual record selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also look-up a single record by id:

  >>> User.find(1)
  # SELECT * FROM users u WHERE u.id = 1 LIMIT 1;
  User(id=1, name='Paul', email='paul@beatl.es', ...)
  >>> User.find_by(values={'name': 'Paul'})
  # SELECT * FROM users u WHERE u.name = 'Paul' LIMIT 1;
  User(id=1, name='Paul', email='paul@beatl.es', ...)

Note that the returned object is a ``Record`` object which allows you to access attributes in those way:

  >>> user['name']
  Paul
  >>> user.name
  Paul


INSERT queries
--------------

  >>> user = User.insert(values={'name': 'Pete', 'email': 'pete@beatl.es'})
  # INSERT INTO users (name, email) VALUES ('Pete', 'pete@beatl.es') RETURNING id;
  # SELECT u.* FROM users WHERE u.id = 4;
  >>> user
  id   name    email
  4    Pete    pete@beatl.es


UPDATE queries
--------------

  >>> users = User.update(values={'hair': 'long'}, where={'name': 'John'})
  # UPDATE users u SET (u.hair) = ('long') WHERE u.name = 'John' RETURNING id;
  # SELECT * FROM users u WHERE u.name = 'John';

DELETE queries
--------------

  >>> User.delete(where={'id': 1})
  # DELETE FROM users u WHERE u.id = 1;

Raw queries
-----------

  >>> jardin.query(sql='SELECT * FROM users WHERE id IN %(ids)s;', params={'ids': [1, 2, 3]})
  # SELECT * FROM users WHERE id IN (1, 2, 3);

Query from SQL file
-------------------

>>> jardin.query(filename='path/to/file.sql', params={...})

The path is relative to the working directory (i.e. where your app was launched).