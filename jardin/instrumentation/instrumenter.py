import time

from jardin.instrumentation.event import Event
from .notifier import Notifer


class Instrumenter:
  def __init__(self, event_name, tags={}):
      self.event_name = event_name
      self.tags = tags
      self.start_time = None
      self.monotonic_start = None

  def __enter__(self):
      self.monotonic_start = time.monotonic()
      self.start_time = time.time()

  def __exit__(self, exc_type, exc_value, exc_traceback):
      start_time = self.start_time
      end_time = time.time()
      duration = time.monotonic() - self.monotonic_start

      Notifer.report_event(Event(self.event_name, start_time, end_time, duration, self.tags))

