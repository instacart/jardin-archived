from types import TracebackType
from typing import *
from dataclasses import dataclass, field

@dataclass
class EventTiming:
    start_time: float
    end_time: float
    duration_seconds: float

@dataclass
class EventExceptionInformation:
    type: type
    exception: Exception
    traceback: TracebackType

@dataclass
class Event:
    name: str
    timing: Optional[EventTiming] = None
    error: Optional[EventExceptionInformation] = None
    tags: Dict[str, str] = field(default_factory=dict)

