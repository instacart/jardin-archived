# jardin

**jardin** *(noun, french)* – garden, yard, grove.

Jardin is a `pandas.DataFrame`-based ORM for Python applications.

[![PyPI version](https://badge.fury.io/py/jardin.svg)](https://badge.fury.io/py/jardin)

## Getting started

### Installation
```shell
$ pip install jardin
```
or
```shell
$ echo 'jardin' >> requirements.txt
$ pip install -r requirements.txt
```

### Setup

In your working directory (the root of your app), create a file named `jardin_conf.py`:
```python
# jardin_conf.py

DATABASES = {
  'my_master_database': 'https://username:password@master_database.url:port',
  'my_replica_database': 'https://username:password@replica_database.url:port'
}

LOG_LEVEL = logging.DEBUG

WATERMARK = 'My Great App'
```
Then, in your app, say you have a table called `users`:
```python
# app.py
import jardin

class Users(jardin.Model):
  db_names = {'master': 'my_master_database', 'replica': 'my_replica_database'}
```
In the console:
```python
>>> from app import Users
>>> users = Users.last(4)
# /* My Great App */ SELECT * FROM users ORDER BY u.created_at DESC LIMIT 4;
>>> users
id   name    email              ...
0    John    john@beatl.es      ...
1    Paul    paul@beatl.es      ...
2    George  george@beatl.es    ...
3    Ringo   ringo@beatl.es     ...
```
The resulting object is a pandas dataframe:
```python
>>> import pandas
>>> isinstance(users, pandas.DataFrame)
True
>>> isinstance(users, jardin.Model)
True
```

## Queries

### SELECT queries

Here is the basic syntax to select records from the database
```python
>>> users = Users.select(select=['id', 'name'], where={'email': 'paul@beatl.es'},
                         order='id ASC', limit=1)
# /* My Great App */ SELECT u.id, u.name FROM users u WHERE u.email = 'paul@beatl.es' ORDER BY u.id ASC LIMIT 1;
>>> users
id   name
1    Paul
```

#### Arguments

* `select` – The list of columns to return. If not provided, all columns will be returned.
* `where` – conditions. Many different formats can be used to provide conditions. See [docs](#where-argument).
* `inner_join`, `left_join` – List of tables to join with their join condition. Can also be a list of classes if the appropriate associations have been declared. See [docs](#inner_join-left_join-arguments).
* `order` – order clause
* `limit` – limit clause
* `group` – grouping clause
* `scopes` – list of pre-defined scopes. See docs.

##### `where` argument

Here are the different ways to feed a condition clause to a query.
* `where = "name = 'John'"`
* `where = {'name': 'John'}`
* `where = {'id': (0, 3)}` – selects where `id` is between 0 and 3
* `where = {'id': [0, 1, 2]}` – selects where `id` is in the array
* `where = [{'id': (0, 10), 'instrument': 'drums'}, ["created_at > %(created_at)s", {'created_at': '1963-03-22'}]]`

##### `inner_join`, `left_join` arguments

The simplest way to join another table is as follows
```python
>>> Users.select(inner_join=["instruments i ON i.id = u.instrument_id"])
```
If you have configured your models associations, see [here](#associations), you can simply pass the class as argument:
```python
>>> Users.select(inner_join=[Instruments])
```

#### Individual record selection
You can also look-up a single record by id:
```python
>>> Users.find(1)
# /* My Great App */ SELECT * FROM users u WHERE u.id = 1 LIMIT 1;
Record(id=1, name='Paul', email='paul@beatl.es', ...)
>>> Users.find_by(values={'name': 'Paul'})
# /* My Great App */ SELECT * FROM users u WHERE u.name = 'Paul' LIMIT 1;
Record(id=1, name='Paul', email='paul@beatl.es', ...)
```
Note that the returned object is a `Record` object which allows you to access attributes in those way:
```python
>>> user['name']
Paul
>>> user.name
Paul
```
It is possible to define your own record classes.
```python
# app.py
import jardin

class User(jardin.Record):

  def is_drummer(self):
    return self.name == 'Ringo'

class Users(jardin.Model):
  record_class = User
```
And then
```python
>>> user = Users.find(1)
>>> user.is_drummer()
False
```
### INSERT queries
```python
>>> user = Users.insert(name='Pete', email='pete@beatl.es')
# /* My Great App */ INSERT INTO users (name, email) VALUES ('Pete', 'pete@beatl.es') RETURNING id;
# /* My Great App */ SELECT u.* FROM users WHERE u.id = 4;
>>> user
id   name    email
4    Pete    pete@beatl.es
```

### UPDATE queries
```python
>>> users = Users.update(values={'hair': 'long'}, where={'name': 'John'})
# /* My Great App */ UPDATE users u SET (u.hair) = ('long') WHERE u.name = 'John' RETURNING id;
# /* My Great App */ SELECT * FROM users u WHERE u.name = 'John';
```
### DELETE queries
```python
>>> Users.delete(where={'id': 1})
# /* My Great App */ DELETE FROM users u WHERE u.id = 1;
```
### Raw queries
```python
>>> Users.query(sql='SELECT * FROM users LIMIT 10;')
# /* My Great App */ SELECT * FROM users LIMIT 10;
```
### Query from SQL file
```python
>>> Users.query(filename='path/to/file.sql')
```
The path is relative to the working directory (i.e. where your app was launched).
## Associations
It is possible to define associations between models. For example, if each user has multiple instruments:

```python
# app.py

import jardin

class MyModel(jardin.Model):
  db_names = {'master': 'my_master_database', 'replica': 'my_replica_database'}

class Instruments(MyModel):
  belongs_to = {'users': 'user_id'}

class Users(MyModel):
  has_many = [Instruments]
```
and then you can query the associated records:
```python
>>> users = Users.select()
# /* My Great App */ SELECT * FROM users u;
>>> instruments = users.instruments()
# /* My Great App */ SELECT * FROM instruments i WHERE i.id IN (0, 1, ...);
```
Or you can declare joins more easily
```python
>>> users = Users.select(inner_join=[Instruments])
```

## Scopes
Queries conditions can be generalized across your app:
```python
# app.py

class Users(jardin.Model):
  scopes = {
    'alive': {'name': ['Paul', 'Ringo']},
    'guitarists': {'name': ['John', 'George']}
  }
```
The key is the name of the scope, and the value is the conditions to be applied. Anything that can be fed to the `where` argument of `Model#select` can be used to define a scope.

Use them as such:
```python
>>> users = Users.select(scopes=['alive'], ...)
# /* My Great App */ SELECT * FROM users u WHERE u.name IN ('Paul', 'Ringo') AND ...;
```
## Misc

### Watermark and trace

A watermark string can be set in the `jardin_conf.py` file. It will be printed alongside the location of the code triggering the query in each log.
```python
# jardin_conf.py
WATERMARK = 'My Great App'
```
And then
```python
# app.py
User.select()
```
Will output
```SQL
/* My Great App:/path/to/app.py:2 */ SELECT * FROM users u;
```

### Multiple databases

If you are reading tables from multiple databases, each with a read-only replica, you can define them as such in your `jardin_conf.py` file:
```python
# jardin_conf.py
DATABASES = {
  'mydb1': 'https://username:password@db1.url:port',
  'mydb1-replica': 'https://username:password@db1-replica.url:port',
  'mydb2': 'https://username:password@db2.url:port',
  'mydb2-replica': 'https://username:password@db2-replica.url:port',
}
```
And then define your models
```python
# app.py
import jardin

class Db1Model(jardin.Model):
  db_names = {'master': 'mydb1', 'replica': 'mydb1-replica'}

class Db2Model(jardin.Model):
  db_names = {'master': 'mydb2', 'replica': 'mydb2-replica'}

class Users(Db1Model): pass

class Instruments(Db2Model): pass
```

### Replica lag measurement

You can measure the current replica lag with
```python
MyModel.replica_lag()
# 0.001
```
