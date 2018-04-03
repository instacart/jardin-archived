import unittest
import pandas
import numpy

from tests import transaction

from tests.models import JardinTestModel


class User(JardinTestModel): pass


class TestInsert(unittest.TestCase):

    @transaction(model=User)
    def test_nat(self):
        df = pandas.DataFrame(columns=['name', 'deleted_at', 'num'])
        df = df.append([{'name': 'user', 'deleted_at': None, 'num': numpy.nan}])
        df = df.append([{'name': 'user2', 'deleted_at': None, 'num': numpy.nan}])
        df['deleted_at'] = pandas.to_datetime(df.deleted_at)
        User.insert(values=df)
        self.assertEqual(User.count(), len(df))


if __name__ == "__main__":
    unittest.main()
