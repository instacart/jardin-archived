import unittest
from datetime import datetime, timedelta
from jardin.comparators import *
from tests import transaction

from tests.models import JardinTestModel

class User(JardinTestModel): pass


class TestComparators(unittest.TestCase):
    
    @transaction(model=User)
    def test_is_not_null(self):
        User.insert(values={'name': 'jardin'})
        User.insert(values={'created_at': datetime.utcnow()})
        self.assertEqual(User.count(), 2)
        self.assertEqual(User.count(where={'name': not_null()}), 1)

    @transaction(model=User)
    def test_greater_than(self):
        User.insert(values={'name': 'jardin'})
        self.assertEqual(
            User.count(
                where={'created_at': lt(datetime.utcnow() + timedelta(hours=1))}
                )
            , 1)
        self.assertEqual(
            User.count(
                where={'created_at': gt(col='updated_at')}
                )
            , 0)


if __name__ == "__main__":
    unittest.main()
