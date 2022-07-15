import time
from types import TracebackType
from typing import Dict, Optional, Type

from erdos.operator import BaseOperator


class Profile:
    """Used to log the duration of a snippet of code using a with statement."""

    def __init__(
        self,
        event_name: str,
        operator: BaseOperator,
        event_data: Optional[Dict[str, str]] = None,
    ) -> None:
        self.event_name = event_name
        self.operator = operator
        if event_data is None:
            self.event_data = {}
        else:
            self.event_data = event_data

    def __enter__(self) -> "Profile":
        """Log the start time of a profile event."""
        self.start_time = time.time()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        for key, value in self.event_data.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError(
                    "The event_data must be a dict mapping strings to strings"
                )
        # Start time in us.
        ts = int(self.start_time * 1000 * 1000)
        # Duration in us.
        dur = int((time.time() - self.start_time) * 1000 * 1000)
        # Log the event in the Google Chrome trace event format.
        event = {
            "name": self.event_name,
            "pid": self.operator.config.name,
            "tid": 1,
            "ts": ts,
            "dur": dur,
            "ph": "X",
            "args": self.event_data,
        }
        self.operator.add_trace_event(event)
