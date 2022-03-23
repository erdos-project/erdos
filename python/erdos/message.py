import pickle
from typing import Any

from erdos.internal import PyMessage
from erdos.timestamp import Timestamp


class Message:
    """A :py:class:`Message` allows an operator to send timestamped data to
    other operators via a :py:class:`WriteStream` or an
    :py:class:`IngestStream`.

    Attributes:
        timestamp: The timestamp of the message.
        data: The data of the message.
    """

    def __init__(self, timestamp: Timestamp, data: Any):
        """Constructs a :py:class:`Message` with the given `data` and
        `timestamp`.

        Args:
            timestamp: The :py:class:`Timestamp` associated with the data.
            data: The payload to be sent on the :py:class:`WriteStream`.
        """
        if not isinstance(timestamp, Timestamp):
            raise TypeError("timestamp must be of type `erdos.Timestamp`")
        self.timestamp = timestamp
        self.data = data
        self._serialized_data = None

    def _serialize_data(self):
        """Serializes the message's data using pickle.

        Allows an application to front-load cost of serializing data, which
        usually occurs when the message is sent, in order to reduce the cost
        of later sending the message.

        If the data is later changed, :py:meth:`Message._serialize_data` must
        be called again to reflect changes in the message.
        """
        self._serialized_data = pickle.dumps(
            self.data, protocol=pickle.HIGHEST_PROTOCOL
        )

    def _to_py_message(self) -> PyMessage:
        """Converts the current message to a :py:class:`PyMessage`.

        Returns:
            The :py:class:`PyMessage` instance representing `self`.
        """
        if self._serialized_data is None:
            self._serialize_data()
        return PyMessage(self.timestamp._to_py_timestamp(), self._serialized_data)

    def __str__(self):
        return "{{timestamp: {}, data: {}}}".format(self.timestamp, self.data)


class WatermarkMessage(Message):
    """A :py:class:`WatermarkMessage` allows an operator to convey the
    completion of all outgoing data for a given timestamp on a
    :py:class:`WriteStream`.

    Attributes:
        timestamp: The timestamp for which this is a watermark.
    """

    def __init__(self, timestamp: Timestamp):
        super(WatermarkMessage, self).__init__(timestamp, None)

    def __str__(self):
        return "{{timestamp: {}, watermark: True}}".format(self.timestamp)

    def _to_py_message(self) -> PyMessage:
        """Converts the current message to a :py:class:`PyMessage`.

        Returns:
            The :py:class:`PyMessage` instance representing self.
        """
        return PyMessage(self.timestamp._to_py_timestamp(), None)

    @property
    def is_top(self) -> bool:
        """Indicates whether the watermark conveyed by this message
        corresponds to the top timestamp.

        Returns:
            `true` if the timestamp is top, `false` otherwise.
        """
        return self.timestamp.is_top
