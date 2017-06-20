# jardin

Jardin is a `pandas.DataFrame`-based ORM for Python applications.

## Getting started

In your working directory (the root of your app), create a file named `jardin_conf.py`:
```python
# jardin_conf.py

DATABASES = {
  'my_first_database': 'https://username:password@database.url:port',
  'my_second_database': 'https://username:password@database.url:port'
}

LOG_LEVEL = logging.DEBUG

WATERMARK = 'My Great App'
```
Then, in your app, say you have a table called `users`:
```python
# app.py
import jardin

class Users(jardin.Model):
  db_names = {'read': 'my_first_database', 'write': 'my_second_database'}
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

## Queries

### SELECT queries
Here is the basic syntax to select records from the database
```python
>>> users = Users.select(select = ['id', 'name'], where = {'email': 'paul@beatl.es'},
                         order = 'id ASC', limit = 1)
# /* My Great App */ SELECT u.id, u.name FROM users u WHERE u.email = 'paul@beatl.es' ORDER BY u.id ASC LIMIT 1;
>>> users
id   name
1    Paul
```
#### Arguments
* __select__ – The list of columns to return. If not provided, all columns will be returned.

### INSERT queries
```python
>>> user = Users.insert(name = 'Pete', email = 'pete@beatl.es')
# /* My Great App */ INSERT INTO users (name, email) VALUES ('Pete', 'pete@beatl.es') RETURNING id;
# /* My Great App */ SELECT u.* FROM users WHERE u.id = 4;
>>> user
id   name    email
4    Pete    pete@beatl.es
```