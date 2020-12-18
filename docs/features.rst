Features
========

..Associations
..------------

..Belongs-to and has-many relationships can be declared as such:

..  class Posts(jardin.Model):
..    belongs_to = {
..      'users': 'user_id'
..    }

..  class Users(jardin.Model):
..    has_many = [Posts]

..And then used as such:

..  users = Users.select()
..  posts = users.posts()

..Or:

..  Posts.select(
..    inner_join=[Users],
..    where={'u.id': 123})

Query watermarking
------------------

By defining a watermark in your ``jardin_conf.py`` file::

  WATERMARK = 'MyGreatApp'

Queries will show up as such in your SQL logs::

  /* MyGreatApp | path/to/file.py:function_name:line_number */ SELECT * FROM ....;

Scopes
------

Query scopes can be defined inside your model as such::

  class User(jardin.Model):

    scopes = {
      'active': {'active': True},
      'recent': ["last_sign_up_at > %(week_ago)s", {'week_ago': datetime.utcnow() - timedelta(weeks=1)}]
    }

Then used as such::

  User.select(scopes = ['active', 'recent'])

Which will issue this statement

.. code-block:: psql

  SELECT * FROM users u WHERE u.active IS TRUE AND u.last_sign_up_at > ...;

Soft deletes
------------
If you don't want to actually remove rows from the database when deleting a record, you can activate soft-deletes::

  class User(jardin.Model):

    soft_delete = True

When the ``destroy`` method is called on a model instance, the ``deleted_at`` database field on the corresponding table will be set to the current UTC time.

Then, when calling ``select``, ``count``, ``delete`` or ``update``, rows with a non-NULL `deleted_at` value will be ignored. This can be overridden by passing the ``skip_soft_delete=True`` argument.

The ``find`` method is not affected.

To force delete a single record, call ``destroy(force=True)``.

To customize the database column used to store the deletion timestamp, do::

  class User(jardin.Model):

    soft_delete = 'my_custom_db_column'



Multiple databases and master/replica split
-------------------------------------------

Multiple databases can be declared in ``jardin_conf.py``::

  DATABASES = {
    'my_first_db': 'postgres://...',
    'my_first_db_replica': 'postgres://...',
    'my_second_db': 'postgres://...',
    'my_second_db_replical': 'postgres://...'
  }

And then in your model declarations::

  class Db1Model(jardin.Model):
    db_name = {'master': 'my_first_db', 'replica': 'my_first_db_replica'}

  class Db2Model(jardin.Model):
    db_name = {'master': 'my_second_db', 'replica': 'my_second_db_replica'}

  class User(Db1Model): pass

  class Project(Db2Model): pass


Replica lag measurement
-----------------------

You can measure the current replica lag in seconds using any class inheriting from ``jardin.Model``::

  jardin.Model.replica_lag()
  # 0.001

  MyModel.replica_lag()
  # 0.001

Connection drops recovery
-------------------------

The exceptions ``psycopg2.InterfaceError`` and ``psycopg2.OperationalError`` are rescued and a new connection is initiated. Three attempts with exponential decay are made before bubbling up the exception.