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

   Attributes:
       coordinates (`Sequence[int]`): An array of coordinates.
       is_bottom (`bool`): Representing Timestamp::Bottom.
       is_top (`bool`): Representing Timestamp::Top.
    """
    def __init__(self,
                 timestamp=None,
                 coordinates: Sequence[int] = None,
                 is_top: bool = False,
                 is_bottom: bool = False,
                 _py_timestamp: PyTimestamp = None):
        if _py_timestamp is not None:
            # Initialize from PyTimestamp, if available.
            self._is_top = _py_timestamp.is_top
            self._is_bottom = _py_timestamp.is_bottom
            self.coordinates = _py_timestamp.coordinates
        elif timestamp is not None:
            # If Timestamp is available, copy its contents.
            self._is_top = timestamp.is_top
            self._is_bottom = timestamp.is_bottom
            self.coordinates = timestamp.coordinates
        else:
            if is_top and not is_bottom and coordinates is None:
                self._is_top = is_top
                self._is_bottom = is_bottom
                self.coordinates = coordinates
            elif is_bottom and not is_top and coordinates is None:
                self._is_top = is_top
                self._is_bottom = is_bottom
                self.coordinates = coordinates
            elif coordinates is not None and not is_bottom and not is_top:
                self._is_top = is_top
                self._is_bottom = is_bottom
                self.coordinates = coordinates
            else:
                raise ValueError("Timestamp should either have coordinates"
                                 "or be either Top or Bottom")

    def _to_py_timestamp(self) -> PyTimestamp:
        """Converts the current timestamp to a :py:class:`PyTimestamp`.

        Returns:
            The :py:class:`PyTimestamp` instance representing self.
        """
        return PyTimestamp(self.coordinates, self.is_top, self.is_bottom)

    def __repr__(self):
        if self.is_top:
            return "Timestamp::Top"
        elif self.is_bottom:
            return "Timestamp::Bottom"
        else:
            return "Timestamp::Time({})".format(str(self.coordinates))

    def __str__(self):
        return self.__repr__()

    def __eq__(self, timestamp):
        if self.is_top and timestamp.is_top:
            return True
        if self.is_bottom and timestamp.is_bottom:
            return True
        if len(self.coordinates) != len(timestamp.coordinates):
            return False
        for coord, other_coord in zip(self.coordinates, timestamp.coordinates):
            if coord != other_coord:
                return False
        return True

    def __ne__(self, timestamp):
        return not self.__eq__(timestamp)

    def __lt__(self, timestamp):
        # Compare Bottom timestamps.
        if self.is_bottom and timestamp.is_bottom:
            return False
        if self.is_bottom and not timestamp.is_bottom:
            return True
        if not self.is_bottom and timestamp.is_bottom:
            return False

        # Compare Top timestamps.
        if self.is_top:
            return False
        if not self.is_top and timestamp.is_top:
            return True

        # Compare coordinates.
        if len(self.coordinates) != len(timestamp.coordinates):
            raise Exception(
                "Cannot compare timestamps of different size {} and {}".format(
                    self, timestamp))
        for coord, other_coord in zip(self.coordinates, timestamp.coordinates):
            if coord > other_coord:
                return False
            elif coord < other_coord:
                return True
        return False

    def __le__(self, timestamp):
        # Compare Bottom timestamps.
        if self.is_bottom:
            return True
        if not self.is_bottom and timestamp.is_bottom:
            return False

        # Compare Top timestamps.
        if self.is_top:
            return timestamp.is_top
        if not self.is_top and timestamp.is_top:
            return True

        # Compare coordinates.
        if len(self.coordinates) != len(timestamp.coordinates):
            raise Exception(
                "Cannot compare timestamps of different size {} and {}".format(
                    self, timestamp))
        for coord, other_coord in zip(self.coordinates, timestamp.coordinates):
            if coord > other_coord:
                return False
            elif coord < other_coord:
                return True
        return True

    def __gt__(self, timestamp):
        return not self.__le__(timestamp)

    def __ge__(self, timestamp):
        return not self.__lt__(timestamp)

    def __hash__(self):
        return hash((self.coordinates, self.is_top, self.is_bottom))

    @property
    def is_top(self):
        return self._is_top

    @property
    def is_bottom(self):
        return self._is_bottom
