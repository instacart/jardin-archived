Querying
========

SELECT queries
--------------

Here is the basic syntax to select records from the database

  >>> users = Users.select(
                select=['id', 'name'],
                where={'email': 'paul@beatl.es'},
                order='id ASC',
                limit=1)
  # /* My Great App */ SELECT u.id, u.name FROM users u WHERE u.email = 'paul@beatl.es' ORDER BY u.id ASC LIMIT 1;
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

**inner_join, left_join arguments**

The simplest way to join another table is as follows

  >>> Users.select(inner_join=["instruments i ON i.id = u.instrument_id"])

If you have configured your models associations, see :doc:`features`, you can simply pass the class as argument::

  >>> Users.select(inner_join=[Instruments])

Individual record selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also look-up a single record by id:

  >>> Users.find(1)
  # /* My Great App */ SELECT * FROM users u WHERE u.id = 1 LIMIT 1;
  Record(id=1, name='Paul', email='paul@beatl.es', ...)
  >>> Users.find_by(values={'name': 'Paul'})
  # /* My Great App */ SELECT * FROM users u WHERE u.name = 'Paul' LIMIT 1;
  Record(id=1, name='Paul', email='paul@beatl.es', ...)

Note that the returned object is a ``Record`` object which allows you to access attributes in those way:

  >>> user['name']
  Paul
  >>> user.name
  Paul

It is possible to define your own record classes.

  # app.py
  import jardin

  class User(jardin.Record):

    def is_drummer(self):
      return self.name == 'Ringo'

  class Users(jardin.Model):
    record_class = User

And then

  >>> user = Users.find(1)
  >>> user.is_drummer()
  False

INSERT queries
--------------

  >>> user = Users.insert(values={'name': 'Pete', 'email': 'pete@beatl.es'})
  # /* My Great App */ INSERT INTO users (name, email) VALUES ('Pete', 'pete@beatl.es') RETURNING id;
  # /* My Great App */ SELECT u.* FROM users WHERE u.id = 4;
  >>> user
  id   name    email
  4    Pete    pete@beatl.es


UPDATE queries
--------------

  >>> users = Users.update(values={'hair': 'long'}, where={'name': 'John'})
  # /* My Great App */ UPDATE users u SET (u.hair) = ('long') WHERE u.name = 'John' RETURNING id;
  # /* My Great App */ SELECT * FROM users u WHERE u.name = 'John';

DELETE queries
--------------

  >>> Users.delete(where={'id': 1})
  # /* My Great App */ DELETE FROM users u WHERE u.id = 1;

Raw queries
-----------

  >>> Users.query(sql='SELECT * FROM users LIMIT 10;')
  # /* My Great App */ SELECT * FROM users LIMIT 10;

Query from SQL file
-------------------

>>> Users.query(filename='path/to/file.sql')

The path is relative to the working directory (i.e. where your app was launched).