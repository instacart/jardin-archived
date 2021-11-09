import time
from contextlib import contextmanager

from jardin.instrumentation.event import Event, EventExceptionInformation, EventTiming
import jardin.config as config


@contextmanager
def instrumention(event_name, tags=None):
    tags = {} if tags is None else tags
    monotonic_start = time.monotonic()
    start_time = time.time()
    exception_info = None
    try:
        yield
    except Exception as e:
        exception_info = EventExceptionInformation(e)
        raise
    finally:
        timing = EventTiming(
          start_time=start_time,
          end_time=time.time(),
          duration_seconds=time.monotonic() - monotonic_start
        )
        config.notifier.report_event(
          Event(name=event_name, timing=timing, error=exception_info, tags=tags)
        )
