import pickle

from erdos.message import Message, WatermarkMessage
from erdos.internal import (PyReadStream, PyWriteStream, PyLoopStream,
                            PyIngestStream, PyExtractStream)
from erdos.timestamp import Timestamp


def _parse_message(internal_msg):
    """Creates a Message from an internal stream's response."""
    time_coordinates, serialized_data = internal_msg
    if serialized_data is None:
        return WatermarkMessage(Timestamp(coordinates=time_coordinates))
    return pickle.loads(serialized_data)


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

    # TODO: match Rust API and pass write streams as arguments?
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

        def internal_watermark_callback(coordinates):
            timestamp = Timestamp(coordinates=coordinates)
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

    def send(self, msg):
        """Sends a message on the stream.

        Args:
            msg (Message): the message to send. This may be a `Watermark` or a
                `Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")

        if isinstance(msg, WatermarkMessage):
            return self._py_write_stream.send_watermark(
                msg.timestamp.coordinates)
        else:
            serialized = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
            return self._py_write_stream.send(msg.timestamp.coordinates,
                                              serialized)


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

    def send(self, msg):
        """Sends a message on the stream.

        Args:
            msg (Message): the message to send. This may be a `Watermark` or a
                `Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")

        if isinstance(msg, WatermarkMessage):
            return self._py_ingest_stream.send_watermark(
                msg.timestamp.coordinates)
        else:
            serialized = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
            return self._py_ingest_stream.send(msg.timestamp.coordinates,
                                               serialized)


class ExtractStream(object):
    """Used to receive messages outside of an operator.

    Args:
        read_stream (ReadStream): the stream from which to read messages.
    """
    def __init__(self, read_stream):
        self._py_extract_stream = PyExtractStream(read_stream._py_read_stream)

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
