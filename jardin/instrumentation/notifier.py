import os
import uuid
import queue
import atexit
import threading

from jardin import config as config
from jardin.instrumentation.base_subscriber import BaseSubscriber

class Notifer:
  # We set maxsize to avoid leaking memory if the reporting thread is dead for any reason
  # in report_event we use `put_nowait` to avoid blocking callers if the queue is full
  _q = queue.Queue(maxsize=4096)
  _subscribers = {}
  _should_stop = False
  _worker_count = 2
  _workers = []

  @classmethod
  def is_enabled(self):
      return os.environ.get("JARDIN_INSTRUMENTATION_ENABLED", "false") == "true"

  @classmethod
  def subscribe(self, handler):
      if not self.is_enabled():
          return
      subscriber_id = uuid.uuid4()
      if not issubclass(type(handler), BaseSubscriber):
          raise TypeError("Handler must be subclass of BaseSubscriber")
      self._subscribers[subscriber_id] = handler
      return subscriber_id

  @classmethod
  def unsubscribe(self, subscriber_id):
      if not self.is_enabled():
          return
      del self._subscribers[subscriber_id]

  @classmethod
  def report_event(self, event_name, start_time=0, end_time=0, duration=0, tags={}):
      if not self.is_enabled() or self._should_stop:
          return
      try:
          self._q.put_nowait((event_name, start_time, end_time, duration, tags))
      except queue.Full:
          config.logger.error("Failed to report event {}".format(event_name))

  @classmethod
  def process_queue(self):
      if not self.is_enabled():
          return
      while True:
          try:
              event_name, start_time, end_time, duration, tags = self._q.get()
              if event_name is None:
                  # We got an exit signal
                  return
              subscribers = dict(self._subscribers)
              for subscriber_id in subscribers:
                  subscriber = subscribers[subscriber_id]
                  try:
                      subscriber.report_event(event_name, start_time, end_time, duration, tags)
                  except Exception as e:
                      self.handle_error(event_name, subscriber, e)
          finally:
              self._q.task_done()

  @classmethod
  def handle_error(self, event_name, subscriber, exc):
      config.logger.error("Failed to report event {} to {} due to {}".format(event_name, str(type(subscriber)), str(exc)))


  @classmethod
  def start(self):
      for _ in range(self._worker_count):
          t = threading.Thread(target=Notifer.process_queue, daemon=True)
          self._workers.append(t)
          t.start()
      atexit.register(self.terminate)

  @classmethod
  def terminate(self):
      self._should_stop = True # prevent events from being enqueued
      self._q.put((None, None, None, None, None)) # plant termination signal
      self.process_queue() # Work through the remaining messages

if Notifer.is_enabled():
    Notifer.start()
