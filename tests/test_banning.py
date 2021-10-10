import time
import unittest
import psycopg2
from jardin.database.client_provider import ClientProvider
from jardin.database.database_adapter import MAX_RETRIES, DatabaseAdapter
from tests.query_tracer import QueryTracer

class TestBanning(unittest.TestCase):

    def test_query_on_all_bad(self):
      provider = ClientProvider('all_bad')
      adapter = DatabaseAdapter(provider, None)

      with self.assertRaises(psycopg2.OperationalError):
        adapter.raw_query(sql="SELECT 1")
      self.assertIsNone(provider.next_client()) # all connections are banned
      time.sleep(1)
      self.assertIsNotNone(provider.next_client()) # the ban is lifted after 1 second

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
        with self.assertRaises(psycopg2.OperationalError):
          adapter.raw_query(sql="SELECT 1")
        query_report = QueryTracer.get_report()
        self.assertEqual(len(query_report["query_list"]),  provider.count() * MAX_RETRIES)
        self.assertEqual(len(query_report["ban_list"]),  provider.count())



if __name__ == "__main__":
    unittest.main()
