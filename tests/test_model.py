import unittest
import pandas

from jardin import Collection
from jardin.model import RecordNotPersisted

from tests import transaction

from models import JardinTestModel


class Project(JardinTestModel):
    belongs_to = {'users': 'user_id'}
class User(JardinTestModel):
    has_many = [Project]
class JardinUser(JardinTestModel): pass


class TestModel(unittest.TestCase):

    def test_table_name_and_alias(self):
        model_metadata = User.model_metadata()
        self.assertEqual(model_metadata['table_name'], 'users')
        self.assertEqual(model_metadata['table_alias'], 'u')
        model_metadata = JardinUser.model_metadata()
        self.assertEqual(model_metadata['table_name'], 'jardin_users')
        self.assertEqual(model_metadata['table_alias'], 'ju')


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
        self.assertEqual(len(users.columns), 4)
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
    def test_relationships(self):
        Project.query(
            sql="CREATE TABLE projects (id serial PRIMARY KEY, user_id integer);"
            )
        user = User.insert(values={'name': 'Jardin'})
        project = Project.insert(values={'user_id': user.id})
        user_projects = user.projects()
        self.assertEqual(len(user_projects), 1)
        self.assertIsInstance(user_projects, Project.collection_class)
        self.assertEqual(user_projects.id.tolist(), [project.id])

if __name__ == "__main__":
    unittest.main()
