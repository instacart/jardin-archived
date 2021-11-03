import uuid
from threading import Lock
from jardin import config as config
from jardin.instrumentation.base_subscriber import BaseSubscriber
from jardin.instrumentation.event import Event

class NullNotifier:
    def subscribe(self, _handler: BaseSubscriber) -> uuid.UUID:
        return uuid.uuid4()

    def unsubscribe(self, _subscriber_id: uuid.UUID) -> None:
        pass

    def report_event(self, ev: Event) -> None:
        pass


class SimpleNotifier:
    def __init__(self):
        self._subscribers = {}
        self.lock = Lock()

    def subscribe(self, handler: BaseSubscriber) -> uuid.UUID:
        with self.lock:
            subscriber_id = uuid.uuid4()
            if not isinstance(handler, BaseSubscriber):
                raise TypeError("Handler must be subclass of BaseSubscriber")
            self._subscribers[subscriber_id] = handler
            return subscriber_id

    def unsubscribe(self, subscriber_id: uuid.UUID) -> None:
        with self.lock:
            del self._subscribers[subscriber_id]

    def report_event(self, ev: Event) -> None:
        with self.lock:
            for subscriber_id in self._subscribers:
                subscriber = self._subscribers[subscriber_id]
                try:
                    subscriber.report_event(ev)
                except Exception as e:
                    self.handle_error(ev.name, subscriber, e)

    def handle_error(self, event_name, subscriber, exc) -> None:
        config.logger.error(f"Failed to report event ({event_name}) to ({type(subscriber)}) due to {exc}")
