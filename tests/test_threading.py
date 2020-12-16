import time
import unittest
from concurrent import futures

from tests import transaction
from tests.models import JardinTestModel


class User(JardinTestModel):
    pass

class TestThreading(unittest.TestCase):

    def _run_query(self, i):
        User.insert(values={'name': f'jardin{i}'})
        time.sleep(0.1)
        user = User.select(where={'name': f'jardin{i}'})
        self.assertEqual(len(user), 1)
        self.assertEqual(len(user.name), 1)
        self.assertEqual(user.name[0], f'jardin{i}')

    @transaction(model=User)
    def test_concurrent_access(self):
        # This test is hardly exhaustive, but it will catch if you are sharing cursors across threads.
        with futures.ThreadPoolExecutor(max_workers=8) as pool:
            tasks = [pool.submit(self._run_query, i) for i in range(12)]
            for task in tasks:
                task.result()


if __name__ == "__main__":
    unittest.main()
