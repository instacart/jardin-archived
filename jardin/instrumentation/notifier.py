import os
import uuid
import queue
import atexit
import threading

from jardin import config as config
from jardin.instrumentation.base_subscriber import BaseSubscriber
from jardin.instrumentation.event import Event

class Notifer:
  _subscribers = {}

  @classmethod
  def is_enabled(self):
      return os.environ.get("JARDIN_INSTRUMENTATION_ENABLED", "false") == "true"

  @classmethod
  def subscribe(self, handler: BaseSubscriber) -> uuid.UUID:
      if not self.is_enabled():
          return
      subscriber_id = uuid.uuid4()
      if not isinstance(handler, BaseSubscriber):
          raise TypeError("Handler must be subclass of BaseSubscriber")
      self._subscribers[subscriber_id] = handler
      return subscriber_id

  @classmethod
  def unsubscribe(self, subscriber_id: uuid.UUID):
      if not self.is_enabled():
          return
      del self._subscribers[subscriber_id]

  @classmethod
  def report_event(self, ev: Event):
      subscribers = dict(self._subscribers)
      for subscriber_id in dict(self._subscribers):
          subscriber = subscribers[subscriber_id]
          try:
              subscriber.report_event(ev)
          except Exception as e:
              self.handle_error(ev.name, subscriber, e)

  @classmethod
  def handle_error(self, event_name, subscriber, exc):
      config.logger.error("Failed to report event {} to {} due to {}".format(event_name, str(type(subscriber)), str(exc)))
