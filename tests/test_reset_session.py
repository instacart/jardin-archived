import unittest

import jardin
from jardin.database import Datasources


class TestResetSession(unittest.TestCase):

    def test_reset_session_does_nothing_when_only_one_db_url_available(self):
        self.assertEqual(len(Datasources.db_configs('jardin_test')), 1)
        client = Datasources.active_client('jardin_test')
        jardin.reset_session()
        client2 = Datasources.active_client('jardin_test')
        self.assertEqual(client, client2)

    def test_reset_session_picks_a_different_client_when_multiple_db_urls_available(self):
        self.assertEqual(len(Datasources.db_configs('multi_url_test')), 2)
        client = Datasources.active_client('multi_url_test')
        jardin.reset_session()
        client2 = Datasources.active_client('multi_url_test')
        self.assertNotEqual(client, client2)


if __name__ == '__main__':
    unittest.main()