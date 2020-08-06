from typing import Any

from erdos.timestamp import Timestamp


class Message(object):
    """A :py:class:`Message` allows an :py:class:`Operator` to send timestamped
    data to other operators via a :py:class:`WriteStream`.

    Attributes:
        timestamp (Timestamp): The timestamp of the message.
        data (Any): The data of the message.
    """
    def __init__(self, timestamp: Timestamp, data: Any):
        """ Construct a :py:class:`Message` with the given `data` and
        `timestamp`.

        Args:
            timestamp: The :py:class:`Timestamp` associated with the data.
            data: The payload to be sent on the :py:class:`WriteStream`.
        """
        if not isinstance(timestamp, Timestamp):
            raise TypeError("timestamp must be of type `erdos.Timestamp`")
        self.timestamp = timestamp
        self.data = data

    def __str__(self):
        return "{{timestamp: {}, data: {}}}".format(self.timestamp, self.data)


class WatermarkMessage(Message):
    """A :py:class:`WatermarkMessage` allows an :py:class:`Operator` to convey
    the completion of all outgoing data for a given timestamp on a
    :py:class:`WriteStream`.

    Attributes:
        timestamp (Timestamp): The timestamp for which this is a watermark.
    """
    def __init__(self, timestamp: Timestamp):
        super(WatermarkMessage, self).__init__(timestamp, None)

    def __str__(self):
        return "{{timestamp: {}, watermark: True}}".format(self.timestamp)

    @property
    def is_top(self) -> bool:
        """ Check if the watermark conveyed by this message corresponds to the
        top timestamp.

        Returns:
            `true` if the timestamp is top, `false` otherwise.
        """
        return self.timestamp.is_top
