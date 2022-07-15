from typing import List, Optional

from erdos.internal import PyTimestamp


class Timestamp:
    """An ERDOS timestamp representing the time for which a
    :py:class:`Message` or :py:class:`WatermarkMessage` is sent.
    """

    def __init__(
        self,
        timestamp: Optional["Timestamp"] = None,
        coordinates: Optional[List[int]] = None,
        is_top: bool = False,
        is_bottom: bool = False,
        _py_timestamp: Optional[PyTimestamp] = None,
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

    def __repr__(self) -> str:
        return str(self._py_timestamp)

    def __str__(self) -> str:
        return repr(self._py_timestamp)

    def __eq__(self, timestamp: object) -> bool:
        if not isinstance(timestamp, Timestamp):
            raise ValueError(f"Equality with '{type(timestamp)}' is not implemented.")
        return self._py_timestamp == timestamp._py_timestamp

    def __ne__(self, timestamp: object) -> bool:
        if not isinstance(timestamp, Timestamp):
            raise ValueError(f"Equality with '{type(timestamp)}' is not implemented.")
        return self._py_timestamp != timestamp._py_timestamp

    def __lt__(self, timestamp: object) -> bool:
        if not isinstance(timestamp, Timestamp):
            raise ValueError(f"Comparison with '{type(timestamp)}' is not implemented.")
        return self._py_timestamp < timestamp._py_timestamp

    def __le__(self, timestamp: object) -> bool:
        if not isinstance(timestamp, Timestamp):
            raise ValueError(f"Comparison with '{type(timestamp)}' is not implemented.")
        return self._py_timestamp <= timestamp._py_timestamp

    def __gt__(self, timestamp: object) -> bool:
        if not isinstance(timestamp, Timestamp):
            raise ValueError(f"Comparison with '{type(timestamp)}' is not implemented.")
        return self._py_timestamp > timestamp._py_timestamp

    def __ge__(self, timestamp: object) -> bool:
        if not isinstance(timestamp, Timestamp):
            raise ValueError(f"Comparison with '{type(timestamp)}' is not implemented.")
        return self._py_timestamp >= timestamp._py_timestamp

    def __hash__(self) -> int:
        py_timestamp_coordinates = self._py_timestamp.coordinates()
        coordinates = (
            tuple(py_timestamp_coordinates)
            if py_timestamp_coordinates is not None
            else None
        )
        return hash((coordinates, self.is_top, self.is_bottom))

    @property
    def coordinates(self) -> Optional[List[int]]:
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
