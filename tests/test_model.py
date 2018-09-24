import unittest
from mock import patch
import pandas as pd
from freezegun import freeze_time
from datetime import datetime, timedelta
from time import sleep
import pandas

from jardin import Collection
from jardin.model import RecordNotPersisted

from tests import transaction

from tests.models import JardinTestModel
from tests.support.mydatetime import _mydatetime


class Project(JardinTestModel):
    belongs_to = {'users': 'user_id'}
class User(JardinTestModel):
    has_many = [Project]
class JardinUser(JardinTestModel): pass

class TestModel(unittest.TestCase):

    def test_table_name_and_alias(self):
        self.assertEqual(User._table_name(), 'users')
        self.assertEqual(User._table_alias(), 'u')
        self.assertEqual(JardinUser._table_name(), 'jardin_users')
        self.assertEqual(JardinUser._table_alias(), 'ju')


    @transaction(model=User)
    def test_select(self):
        users = User.select()
        self.assertIsInstance(users, Collection)
        self.assertIsInstance(users, pandas.DataFrame)
        User.insert(values={'name': 'Jardin'})
        User.insert(values={'name': 'Holberton'})
        users = User.select()
        self.assertEqual(len(users), 2)
        self.assertTrue('Jardin' in users.name.tolist())
        self.assertTrue('Holberton' in users.name.tolist())
        self.assertEqual(len(users.columns), 7)
        self.assertTrue('name' in users.columns)

    @transaction(model=User)
    def test_select_select(self):
        User.insert(values={'name': 'Jardin'})
        users = User.select(select='name')
        self.assertEqual(len(users.columns), 1)
        self.assertTrue('name' in users.columns)
        users = User.select(select=['id', 'name'])
        self.assertEqual(len(users.columns), 2)
        self.assertTrue('name' in users.columns)
        self.assertTrue('id' in users.columns)

    @transaction(model=User)
    def test_select_where(self):
        User.insert(values={'name': 'Jardin'})
        User.insert(values={'name': 'Holberton'})
        users = User.select(where={'name': 'Jardin'})
        self.assertEqual(len(users), 1)
        self.assertTrue('Jardin' in users.name.tolist())
        users = User.select(where=[
            ['name = %(name)s', {'name': 'Jardin'}]
            ])
        self.assertEqual(len(users), 1)
        self.assertTrue('Jardin' in users.name.tolist())

    @transaction(model=User)
    def test_select_list(self):
        User.insert(values={'name': 'jardin'})
        self.assertEqual(User.count(where={'name': ['jardin']}), 1)
        self.assertEqual(User.count(where={'name': pd.Series(['jardin'])}), 1)

    @transaction(model=User)
    def test_count(self):
        User.insert(values={'name': 'Jardin'})
        self.assertEqual(User.count(), 1)

    @transaction(model=User)
    def test_find(self):
        user = User.insert(values={'name': 'Jardin'})
        self.assertIsInstance(user, User)
        user = User.find(user.id)
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, 'Jardin')

    @transaction(model=User)
    def test_find_by(self):
        user = User.insert(values={'name': 'Jardin'})
        self.assertIsInstance(user, User)
        user = User.find_by(values={'name': user.name})
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, 'Jardin')

    @transaction(model=User)
    def test_attributes(self):
        self.assertIsNone(User().id)
        self.assertEqual(User(name='Jardin').name, 'Jardin')
        user = User()
        user.name = 'Jardin'
        self.assertEqual(user.name, 'Jardin')

    @transaction(model=User)
    def test_destroy(self):
        with self.assertRaises(RecordNotPersisted):
            User().destroy()
        user = User(name='jardin')
        user.save()
        self.assertEqual(User.count(), 1)
        user.destroy()
        self.assertEqual(User.count(), 0)

    @transaction(model=User)
    def test_soft_delete(self):
        User.soft_delete = True
        user = User.insert(values={'name': 'Jardin'})
        self.assertIsNone(user.deleted_at)
        user.destroy()
        self.assertEqual(User.count(), 0)
        self.assertEqual(User.count(where="deleted_at IS NOT NULL"), 1)
        self.assertEqual(len(User.select()), 0)
        User.soft_delete = False

    @transaction(model=User)
    def test_soft_delete_column(self):
        User.soft_delete = 'destroyed_at'
        user = User.insert(values={'name': 'Jardin'})
        self.assertIsNone(user.destroyed_at)
        user.destroy()
        self.assertEqual(User.count(), 0)
        self.assertEqual(User.count(where="destroyed_at IS NOT NULL"), 1)
        self.assertEqual(len(User.select()), 0)
        user.destroy(force=True)
        self.assertEqual(User.count(), 0)
        self.assertEqual(User.count(where="destroyed_at IS NOT NULL"), 0)
        User.soft_delete = False

    @transaction(model=User, extra_tables=['projects'])
    def test_relationships(self):
        if Project.db().db_config.scheme == 'sqlite':
            Project.query(
                sql="CREATE TABLE projects (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id integer);"
                )
        else:
            Project.query(
                sql="CREATE TABLE projects (id serial PRIMARY KEY, user_id integer);"
                )
        user = User.insert(values={'name': 'Jardin'})
        project = Project.insert(values={'user_id': user.id})
        user_projects = user.projects()
        self.assertEqual(len(user_projects), 1)
        self.assertIsInstance(user_projects, Project.collection_class)
        self.assertEqual(user_projects.id.tolist(), [project.id])

    @transaction(model=User)
    def test_having(self):
        User.insert(values={'name': 'Jardin'})
        User.insert(values={'name': 'Jardin'})
        User.insert(values={'name': 'Potager'})
        self.assertEqual(User.count(), 3)
        users = User.select(select='name', group='name', having='COUNT(*) > 1')
        self.assertEqual(len(users), 1)

    @transaction(model=User)
    def test_touch(self):
        user = User.insert(values={'name': 'Jardin'})
        self.assertEqual(user.created_at, user.updated_at)
        with freeze_time(datetime.utcnow() + timedelta(hours=1)):
            user.touch()
            self.assertTrue(user.updated_at > user.created_at)
            updated_at = user.updated_at
            with freeze_time(datetime.utcnow() + timedelta(hours=1)):
                User.touch(where={'id': user.id})
                user.reload()
                self.assertTrue(user.updated_at > updated_at)

if __name__ == "__main__":
    unittest.main()
