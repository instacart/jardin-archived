from typing import *
from dataclasses import dataclass, field

@dataclass
class Event:
    name: str
    start_time: float = 0.0
    end_time: float = 0.0
    duration: float = 0.0
    tags: Dict[str, str] = field(default_factory=dict)
