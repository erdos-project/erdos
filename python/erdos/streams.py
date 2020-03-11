import pickle

from erdos.message import Message, WatermarkMessage
from erdos.internal import (PyReadStream, PyWriteStream, PyLoopStream,
                            PyIngestStream, PyExtractStream, PyMessage)
from erdos.timestamp import Timestamp


def _parse_message(internal_msg):
    """Creates a Message from an internal stream's response.

    Args:
        internal_msg (PyMessage): The internal message to parse.
    """
    if internal_msg.is_timestamped_data():
        return pickle.loads(internal_msg.data)
    if internal_msg.is_watermark():
        return WatermarkMessage(
            Timestamp(coordinates=internal_msg.timestamp,
                      is_top=internal_msg.is_top_watermark()))
    raise Exception("Unable to parse message")


def _to_py_message(msg):
    """Converts a Message to an internal PyMessage."""
    if isinstance(msg, WatermarkMessage):
        return PyMessage(msg.timestamp.coordinates, msg.is_top, None)
    else:
        data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
        return PyMessage(msg.timestamp.coordinates, False, data)


class ReadStream(object):
    """Reads data and invokes callbacks when messages are received.

    Handles deserialization of messages, and wraps an `internal.PyReadStream`.

    Currently, no callbacks are invoked while `Operator.run` is executing.

    `_py_read_stream` is set during erdos.run(), and should never be set
    manually.
    """
    def __init__(self, _py_read_stream=None):
        self._py_read_stream = PyReadStream(
        ) if _py_read_stream is None else _py_read_stream

    def is_closed(self):
        """Whether a top watermark message has been received."""
        return self._py_read_stream.is_closed()

    def read(self):
        """Blocks until a message is read from the stream."""
        return _parse_message(self._py_read_stream.read())

    def try_read(self):
        """Tries to read a mesage from the stream.

        Returns None if no messages are available at the moment.
        """
        internal_msg = self._py_read_stream.try_read()
        if internal_msg is None:
            return None
        return _parse_message(internal_msg)

    def add_callback(self, callback, write_streams=None):
        """Adds a callback to the stream.

        Args:
            callback (Message, list of WriteStream -> None): callback that
                takes a message.
            write_streams (list of WriteStream): write streams passed to the
                callback.
        """
        if write_streams is None:
            write_streams = []

        def internal_callback(serialized):
            msg = pickle.loads(serialized)
            callback(msg, *write_streams)

        self._py_read_stream.add_callback(internal_callback)

    def add_watermark_callback(self, callback, write_streams=None):
        """Adds a watermark callback to the stream.

        Args:
            callback (Message, list of WriteStream -> None): callback that
                takes a message.
            write_streams (list of WriteStream): write streams passed to the
                callback.
        """
        if write_streams is None:
            write_streams = []

        def internal_watermark_callback(coordinates, is_top):
            timestamp = Timestamp(coordinates=coordinates, is_top=is_top)
            callback(timestamp, *write_streams)

        self._py_read_stream.add_watermark_callback(
            internal_watermark_callback)


class WriteStream(object):
    """Sends data and invokes callbacks when messages are received.

    Handlese serialization of messages, and wraps an `internal.PyWriteStream`.

    `_py_write_stream` is set during erdos.run(), and should never be set
    manually.
    """
    def __init__(self, _py_write_stream=None):
        self._py_write_stream = PyWriteStream(
        ) if _py_write_stream is None else _py_write_stream

    def is_closed(self):
        """Whether a top watermark message has been sent."""
        return self._py_write_stream.is_closed()

    def send(self, msg):
        """Sends a message on the stream.

        Args:
            msg (Message): the message to send. This may be a `Watermark` or a
                `Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")

        internal_msg = _to_py_message(msg)
        return self._py_write_stream.send(internal_msg)


class LoopStream(object):
    """Stream placeholder used to construct loops in the dataflow graph.

    Must call `set` on a ReadStream to complete the loop.
    """
    def __init__(self):
        self._py_loop_stream = PyLoopStream()

    def set(self, read_stream):
        self._py_loop_stream.set(read_stream._py_read_stream)


class IngestStream(object):
    """Used to send messages from outside of operators."""
    def __init__(self):
        self._py_ingest_stream = PyIngestStream(0)

    def is_closed(self):
        """Whether the stream is closed.

        Returns True if the a top watermark message was sent or the
        IngestStream was unable to successfully set up.
        """
        return self._py_ingest_stream.is_closed()

    def send(self, msg):
        """Sends a message on the stream.

        Args:
            msg (Message): the message to send. This may be a `Watermark` or a
                `Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")

        internal_msg = _to_py_message(msg)
        self._py_ingest_stream.send(internal_msg)


class ExtractStream(object):
    """Used to receive messages outside of an operator.

    Args:
        read_stream (ReadStream): the stream from which to read messages.
    """
    def __init__(self, read_stream):
        self._py_extract_stream = PyExtractStream(read_stream._py_read_stream)

    def is_closed(self):
        """Whether the stream is closed.

        Returns True if the a top watermark message was sent or the
        ExtractStream was unable to successfully set up.
        """
        return self._py_extract_stream.is_closed()

    def read(self):
        """Blocks until a message is read from the stream."""
        return _parse_message(self._py_extract_stream.read())

    def try_read(self):
        """Tries to read a mesage from the stream.

        Returns None if no messages are available at the moment.
        """
        internal_msg = self._py_extract_stream.try_read()
        if internal_msg is None:
            return None
        return _parse_message(internal_msg)
