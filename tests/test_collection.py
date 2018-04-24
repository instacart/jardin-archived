import unittest
import pandas

from jardin import Collection

from tests.models import JardinTestModel


class UserCollection(Collection): pass

class User(JardinTestModel):
    collection_class = UserCollection


class TestCollection(unittest.TestCase):

    def test_model_class(self):
        df = User.collection_instance({'a': [0, 1]})
        self.assertEqual(df.model_class, User)
        df = df[df.a == 1]
        self.assertEqual(df.model_class, User)

    def test_collection_class(self):
        df = User.collection_instance({'a': [0, 1]})
        self.assertIsInstance(df, UserCollection)
        df = df[df.a == 1]
        self.assertIsInstance(df, UserCollection)

    def test_concat(self):
        self.assertEqual(
            pandas.concat(
                [User.collection_instance(), User.collection_instance()]
                ).model_class,
            User
            )

    def test_append(self):
        self.assertEqual(
            User.collection_instance().append(User.collection_instance()).model_class,
            User
            )


if __name__ == "__main__":
    unittest.main()
