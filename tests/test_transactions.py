import unittest
from memoized_property import memoized_property

from tests import TestTransaction

from tests.models import JardinTestModel


class User(JardinTestModel): pass

class TestException(Exception): pass

class TestTransactions(unittest.TestCase):

    @memoized_property
    def transaction_tool(self):
        return TestTransaction(User)

    def test_pass(self):
        self.transaction_tool.setup()
        with User.transaction():
            count = User.count()
            User.insert(values={'name': 'jardin'})
            self.assertEqual(User.count(), count + 1)
        self.assertEqual(User.count(), count + 1)
        self.transaction_tool.teardown()

    def test_rollback(self):
        self.transaction_tool.setup()
        try:
            with User.transaction():
                count = User.count()
                User.insert(values={'name': 'jardin'})
                self.assertEqual(User.count(), count + 1)
                raise TestException()
        except TestException:
            pass
        self.assertEqual(User.count(), count)
        self.transaction_tool.teardown()

if __name__ == "__main__":
    unittest.main()
