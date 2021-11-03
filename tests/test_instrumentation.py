import unittest
from jardin.database.client_provider import ClientProvider
from jardin.database.database_adapter import DatabaseAdapter, NoAvailableConnectionsError
from jardin.database.datasources import Datasources
from jardin.instrumentation.base_subscriber import BaseSubscriber
from jardin import config as config


class TestSubscriber(BaseSubscriber):
    def __init__(self):
        self.published_events = []

    def report_event(self, event):
        self.published_events.append(event)


class TestInstrumentation(unittest.TestCase):
    def setUp(self):
        self.subscriber = TestSubscriber()
        self.notifier_id = config.notifier.subscribe(self.subscriber)

    def tearDown(self):
        config.notifier.unsubscribe(self.notifier_id)
        self.subscriber = None

    def is_banning_disabled(self):
        return Datasources.db_config('jardin_test').scheme == "sqlite"

    def test_query_on_all_bad(self):
        if self.is_banning_disabled():
            return True

        provider = ClientProvider('all_bad')
        adapter = DatabaseAdapter(provider, None)

        with self.assertRaises(NoAvailableConnectionsError):
            adapter.raw_query(sql="SELECT 1")

        found_no_available_connections_raised_event = False
        connection_banned_event_count = 0
        for event in self.subscriber.published_events:
            if event.name == "no_available_connections_raised":
                found_no_available_connections_raised_event = True
                self.assertIsNotNone(event.error)
            if event.name == "connection_banned":
                connection_banned_event_count += 1
                self.assertIsNotNone(event.error)

        self.assertTrue(connection_banned_event_count == provider.connection_count())
        self.assertTrue(found_no_available_connections_raised_event)

    def test_query_on_some_bad(self):
        if self.is_banning_disabled():
            return True

        provider = ClientProvider('some_bad')
        adapter = DatabaseAdapter(provider, None)
        adapter.raw_query(sql="SELECT 1")

        found_query_event = False
        for event in self.subscriber.published_events:
            if event.name == "query":
              found_query_event = True
        self.assertTrue(found_query_event)


if __name__ == "__main__":
    unittest.main()
