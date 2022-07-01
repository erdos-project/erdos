from typing import List, Sequence

from erdos.internal import PyTimestamp


class Timestamp:
    """An ERDOS timestamp representing the time for which a
    :py:class:`Message` or :py:class:`WatermarkMessage` is sent.
    """

    def __init__(
        self,
        timestamp=None,
        coordinates: Sequence[int] = None,
        is_top: bool = False,
        is_bottom: bool = False,
        _py_timestamp: PyTimestamp = None,
    ):
        """Constructs a :py:class:`Timestamp`.

        Args:
            timestamp: Constructs a :py:class:`Timestamp` from another
                :py:class:`Timestamp`
            coordinates: Constructs a :py:class:`Timestamp` from a sequence of
                integers representing the time.
            is_top: Constructs a :py:class:`Timestamp` representing the final
                point in time.
            is_bottom: Constructs a :py:class:`Timestamp` representing the
                initial point in time.
            _py_timestamp: Constructs a :py:class:`Timestamp` from an internal
                timestamp object. This argument should not be provided by the
                user.
        """
        if _py_timestamp is not None:
            # Initialize from PyTimestamp, if available.
            self._py_timestamp = _py_timestamp
        elif timestamp is not None:
            # If Timestamp is available, copy its contents.
            self._py_timestamp = timestamp._py_timestamp
        else:
            if is_top and not is_bottom and coordinates is None:
                self._py_timestamp = PyTimestamp(coordinates, is_top, is_bottom)
            elif is_bottom and not is_top and coordinates is None:
                self._py_timestamp = PyTimestamp(coordinates, is_top, is_bottom)
            elif coordinates is not None and not is_bottom and not is_top:
                self._py_timestamp = PyTimestamp(coordinates, is_top, is_bottom)
            else:
                raise ValueError(
                    "Timestamp should either have coordinates"
                    "or be either Top or Bottom"
                )

    def _to_py_timestamp(self) -> PyTimestamp:
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
    def coordinates(self) -> List[int]:
        """A list of integers representing the time."""
        return self._py_timestamp.coordinates()

    @property
    def is_top(self) -> bool:
        """Whether the timestamp represents the final point in time."""
        return self._py_timestamp.is_top()

    @property
    def is_bottom(self) -> bool:
        """Whether the timestamp represents the initial point in time."""
        return self._py_timestamp.is_bottom()
