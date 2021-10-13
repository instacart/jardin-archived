import time
import unittest
import psycopg2
from jardin.database.client_provider import ClientProvider
from jardin.database.database_adapter import DatabaseAdapter, NoAvailableConnectionsError
from tests.query_tracer import QueryTracer


class TestBanning(unittest.TestCase):

    def test_query_on_all_bad(self):
        provider = ClientProvider('all_bad')
        adapter = DatabaseAdapter(provider, None)

        with self.assertRaises(NoAvailableConnectionsError):
            adapter.raw_query(sql="SELECT 1")
        self.assertIsNone(provider.next_client())  # all connections are banned
        time.sleep(adapter.ban_time)
        # the ban is lifted after ban_time seconds elapse
        self.assertIsNotNone(provider.next_client())

    def test_query_on_some_bad(self):
        provider = ClientProvider('some_bad')
        adapter = DatabaseAdapter(provider, None)
        adapter.raw_query(sql="SELECT 1")
        self.assertIsNotNone(provider.next_client())

    def test_retries(self):
        query_report = None
        with QueryTracer():
            provider = ClientProvider('all_bad')
            adapter = DatabaseAdapter(provider, None)
            with self.assertRaises(NoAvailableConnectionsError):
                adapter.raw_query(sql="SELECT 1")
            query_report = QueryTracer.get_report()

            expected_query_attempts =  provider.connection_count() * adapter.max_retries
            expected_connection_bans = provider.connection_count()
            self.assertEqual(len(query_report["query_list"]), expected_query_attempts)
            self.assertEqual(len(query_report["ban_list"]),  expected_connection_bans)


if __name__ == "__main__":
    unittest.main()
