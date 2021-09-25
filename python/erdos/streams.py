import pickle
import logging
from typing import Union

from erdos.message import Message, WatermarkMessage
from erdos.internal import (PyReadStream, PyWriteStream, PyLoopStream,
                            PyStream, PyIngestStream, PyExtractStream)
from erdos.timestamp import Timestamp

logger = logging.getLogger(__name__)


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


class Stream(object):
    """A :py:class:`Stream` enables the driver to connect operators.

    A :py:class`Stream` is returned by the :py:func`connect` method as an
    abstraction of the :py:class:`WriteStream` of an operator, and can be
    passed to the :py:func:`connect` method to allow other operators to read
    data from it.
    """
    def __init__(self,
                 _py_stream: PyStream = None,
                 _name: Union[str, None] = None,
                 _id: Union[str, None] = None):
        logger.debug(
            "Initializing a Stream with the name: {} and ID: {}.".format(
                _name, _id))
        self._py_stream = _py_stream
        self._name = _name
        self._id = _id


class ReadStream(object):
    """ A :py:class:`ReadStream` allows an :py:class:`Operator` to read and
    do work on data sent by other operators on a corresponding
    :py:class:`WriteStream`.

    An :py:class:`Operator` that takes control of its execution using the
    :py:func:`Operator.run` method can retrieve the messages on a
    :py:class:`ReadStream` using the :py:func:`ReadStream.read` or
    :py:func:`ReadStream.try_read` methods.

    Note:
        No callbacks are invoked if an operator takes control of the execution
        in :py:func:`Operator.run`.
    """
    def __init__(self,
                 _py_read_stream: PyReadStream = None,
                 _name: Union[str, None] = None,
                 _id: Union[str, None] = None):
        logger.debug(
            "Initializing ReadStream with the name: {}, and ID: {}.".format(
                _name, _id))
        self._py_read_stream = PyReadStream(
        ) if _py_read_stream is None else _py_read_stream
        self._name = _name
        self._id = _id

    @property
    def name(self) -> Union[str, None]:
        """ The name of the stream. `None` if no name was given. """
        return self._name

    def is_closed(self) -> bool:
        """Whether a top watermark message has been received."""
        return self._py_read_stream.is_closed()

    def read(self) -> Message:
        """Blocks until a message is read from the stream."""
        return _parse_message(self._py_read_stream.read())

    def try_read(self) -> Union[Message, None]:
        """Tries to read a mesage from the stream.

        Returns None if no messages are available at the moment.
        """
        internal_msg = self._py_read_stream.try_read()
        if internal_msg is None:
            return None
        return _parse_message(internal_msg)


class WriteStream(object):
    """ A :py:class:`WriteStream` allows an :py:class:`Operator` to send
    messages and watermarks to other operators that connect to the
    corresponding :py:class:`ReadStream`.

    Note:
        `_py_write_stream` is set during :py:func:`run`, and should never be
        set manually.
    """
    def __init__(self,
                 _py_write_stream: PyWriteStream = None,
                 _name: Union[str, None] = None,
                 _id: Union[str, None] = None):
        self._py_write_stream = PyWriteStream(
        ) if _py_write_stream is None else _py_write_stream
        self._name = _name
        self._id = _id

    @property
    def name(self) -> Union[str, None]:
        """ The name of the stream. `None` if no name was given. """
        return self._name

    def is_closed(self) -> bool:
        """Whether a top watermark message has been sent."""
        return self._py_write_stream.is_closed()

    def send(self, msg: Message):
        """Sends a message on the stream.

        Args:
            msg: the message to send. This may be a `Watermark` or a `Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")

        internal_msg = msg._to_py_message()
        logger.debug("Sending message {} on the stream {}".format(
            msg, self._name))

        # Raise exception with the name.
        try:
            return self._py_write_stream.send(internal_msg)
        except Exception as e:
            raise Exception("Exception on stream {} ({})".format(
                self._name, self._id)) from e


class LoopStream(object):
    """Stream placeholder used to construct loops in the dataflow graph.

    Note:
        Must call `set` with a valid :py:class:`Stream` to complete the loop.
    """
    def __init__(self, _name: Union[str, None] = None):
        self._py_loop_stream = PyLoopStream()
        self._name = _name

    @property
    def name(self) -> Union[str, None]:
        """ The name of the stream. `None` if no name was given. """
        return self._name

    def connect_loop(self, stream: Stream):
        logger.debug("Setting the read stream {} to the loop stream {}".format(
            stream._name, self._name))
        self._py_loop_stream.connect_loop(stream._py_stream)


class IngestStream(object):
    """An :py:class:`IngestStream` enables drivers to inject data into a
    running ERDOS application.

    The driver can initialize a new :py:class:`IngestStream` and connect it to
    an :py:class:`Operator` through the `connect` family of functions. Similar
    to a :py:class:`WriteStream`, an :py:class:`IngestStream` provides a
    :py:func:`IngestStream.send` to enable the driver to send data to the
    operator to which it was connected.
    """
    def __init__(self, _name: Union[str, None] = None):
        self._py_ingest_stream = PyIngestStream(_name)
        self._name = _name

    @property
    def name(self) -> Union[str, None]:
        """ The name of the stream. `None` if no name was given. """
        return self._name

    def is_closed(self) -> bool:
        """Whether the stream is closed.

        Returns True if the a top watermark message was sent or the
        IngestStream was unable to successfully set up.
        """
        return self._py_ingest_stream.is_closed()

    def send(self, msg: Message):
        """Sends a message on the stream.

        Args:
            msg: the message to send. This may be a
            :py:class:`WatermarkMessage` or a :py:class:`Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")

        logger.debug("Sending message {} on the Ingest stream {}".format(
            msg, self._name))

        internal_msg = msg._to_py_message()
        self._py_ingest_stream.send(internal_msg)


class ExtractStream(object):
    """An :py:class:`ExtractStream` enables drivers to read data from a running
    ERDOS applications.

    The driver can initialize a new :py:class:`ExtractStream` by passing the
    instance of :py:class:`Stream` returned by :py:func:`connect`. Similar
    to a :py:class:`ReadStream`, an :py:class:`ExtractStream` provides
    :py:func:`ExtractStream.read` and :py:func:`ExtractStream.try_read` for
    reading data published on the corresponding `stream`.

    Args:
        stream (:py:class:`Stream`): The stream from which to read messages.
    """
    def __init__(self, stream: Stream):
        if not isinstance(stream, Stream):
            raise ValueError(
                "ExtractStream needs to be initialized with a Stream. "
                "Received a {}".format(type(stream)))
        self._py_extract_stream = PyExtractStream(stream._py_stream, )

    @property
    def name(self) -> Union[str, None]:
        """ The name of the stream. `None` if no name was given. """
        return self._name

    def is_closed(self) -> bool:
        """Whether the stream is closed.

        Returns True if the a top watermark message was sent or the
        :py:class:`ExtractStream` was unable to successfully set up.
        """
        return self._py_extract_stream.is_closed()

    def read(self) -> Message:
        """Blocks until a message is read from the stream."""
        return _parse_message(self._py_extract_stream.read())

    def try_read(self) -> Union[Message, None]:
        """Tries to read a mesage from the stream.

        Returns None if no messages are available at the moment.
        """
        internal_msg = self._py_extract_stream.try_read()
        if internal_msg is None:
            return None
        return _parse_message(internal_msg)
