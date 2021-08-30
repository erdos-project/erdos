from typing import Sequence
from erdos.internal import PyTimestamp


class Timestamp(object):
    """An ERDOS timestamp representing the time for which the messages or
   watermarks are sent.

   A :py:class:`Timestamp` can have one of the following three values:
    - Timestamp::Bottom: Representing the initial time, and used for
        initializing operators.
    - Timestamp::Time(c): An integer timestamp representing the time attached
        to the message or watermark.
    - Timestamp::Top: The final timestamp, and used for destruction of
        operators.
    """
    def __init__(self,
                 timestamp=None,
                 coordinates: Sequence[int] = None,
                 is_top: bool = False,
                 is_bottom: bool = False,
                 _py_timestamp: PyTimestamp = None):
        if _py_timestamp is not None:
            # Initialize from PyTimestamp, if available.
            self._py_timestamp = _py_timestamp
        elif timestamp is not None:
            # If Timestamp is available, copy its contents.
            self._py_timestamp = timestamp._py_timestamp
        else:
            if is_top and not is_bottom and coordinates is None:
                self._py_timestamp = PyTimestamp(coordinates,
                                                 is_top, is_bottom)
            elif is_bottom and not is_top and coordinates is None:
                self._py_timestamp = PyTimestamp(coordinates,
                                                 is_top, is_bottom)
            elif coordinates is not None and not is_bottom and not is_top:
                self._py_timestamp = PyTimestamp(coordinates,
                                                 is_top, is_bottom)
            else:
                raise ValueError("Timestamp should either have coordinates"
                                 "or be either Top or Bottom")

    def _to_py_timestamp(self) -> PyTimestamp:
        """Converts the current timestamp to a :py:class:`PyTimestamp`.

        Returns:
            The :py:class:`PyTimestamp` instance representing self.
        """
        return self._py_timestamp

    def __repr__(self):
        return str(self._py_timestamp)

    def __str__(self):
        return repr(self._py_timestamp)

    def __eq__(self, timestamp):
        return self._py_timestamp == timestamp._py_timestamp

    def __ne__(self, timestamp):
        return self._py_timestamp != timestamp._py_timestamp

    def __lt__(self, timestamp):
        return self._py_timestamp < timestamp._py_timestamp

    def __le__(self, timestamp):
        return self._py_timestamp <= timestamp._py_timestamp

    def __gt__(self, timestamp):
        return self._py_timestamp > timestamp._py_timestamp

    def __ge__(self, timestamp):
        return self._py_timestamp >= timestamp._py_timestamp

    def __hash__(self):
        coordinates = self._py_timestamp.coordinates()
        if coordinates is not None:
            coordinates = tuple(coordinates)
        return hash((coordinates, self.is_top, self.is_bottom))

    @property
    def is_top(self):
        return self._py_timestamp.is_top()

    @property
    def is_bottom(self):
        return self._py_timestamp.is_bottom()
