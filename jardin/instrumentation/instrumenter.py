import time

from jardin.instrumentation.event import Event, EventExceptionInformation, EventTiming
import jardin.config as config

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
      timing = EventTiming(
        start_time=self.start_time,
        end_time=time.time(),
        duration_seconds=time.monotonic() - self.monotonic_start
      )

      exception_info = None
      if exc_type is not None:
          exception_info = EventExceptionInformation(
            type=exc_type,
            exception=exc_value,
            traceback=exc_traceback
          )

      config.notifier.report_event(
          Event(
              name=self.event_name,
              timing=timing,
              error=exception_info,
              tags=self.tags
          )
      )
