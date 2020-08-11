import pickle
import logging
from operator import attrgetter
from typing import Union, Callable

from erdos.message import Message, WatermarkMessage
from erdos.internal import (PyReadStream, PyWriteStream, PyLoopStream,
                            PyIngestStream, PyExtractStream, PyMessage)
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


def _to_py_message(msg):
    """Converts a Message to an internal PyMessage."""
    if isinstance(msg, WatermarkMessage):
        return PyMessage(msg.timestamp.coordinates, msg.is_top, None)
    else:
        data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
        return PyMessage(msg.timestamp.coordinates, False, data)


class ReadStream(object):
    """ A :py:class:`ReadStream` allows an :py:class:`Operator` to read and
    do work on data sent by other operators on a corresponding
    :py:class:`WriteStream`.

    An :py:class:`Operator` can interface with a :py:class:`ReadStream` by
    registering callbacks on the data or watermark received on the stream by
    invoking the :py:func:`add_callback` or :py:func:`add_watermark_callback`
    method respectively.

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

    def add_callback(self, callback: Callable, write_streams=None):
        """Adds a callback to the stream.

        Args:
            callback: A callback that takes a message and a sequence of
                :py:class:`WriteStream` s.
            write_streams: Write streams passed to the callback.
        """
        if write_streams is None:
            write_streams = []

        cb_name = callback.__name__ if "__name__" in dir(callback) else "None"

        logger.debug("Adding callback {name} to the input stream {_input}, "
                     "and passing the output streams: {_output}".format(
                         name=cb_name,
                         _input=self._name,
                         _output=list(map(attrgetter("_name"),
                                          write_streams))))

        def internal_callback(serialized):
            msg = pickle.loads(serialized)
            callback(msg, *write_streams)

        self._py_read_stream.add_callback(internal_callback)

    def add_watermark_callback(self, callback: Callable, write_streams=None):
        """Adds a watermark callback to the stream.

        Args:
            callback: A callback that takes a message and a sequence of
                :py:class:`WriteStream` s.
            write_streams: Write streams passed to the callback.
        """
        if write_streams is None:
            write_streams = []

        cb_name = callback.__name__ if "__name__" in dir(callback) else "None"

        logger.debug(
            "Adding watermark callback {name} to the input stream "
            "{_input}, and passing the output streams: {_output}".format(
                name=cb_name,
                _input=self._name,
                _output=list(map(attrgetter("_name"), write_streams))))

        def internal_watermark_callback(coordinates, is_top):
            timestamp = Timestamp(coordinates=coordinates, is_top=is_top)
            callback(timestamp, *write_streams)

        self._py_read_stream.add_watermark_callback(
            internal_watermark_callback)


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

        internal_msg = _to_py_message(msg)
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

    Must call `set` on a ReadStream to complete the loop.
    """
    def __init__(self, _name: Union[str, None] = None):
        self._py_loop_stream = PyLoopStream()
        self._name = _name

    @property
    def name(self) -> Union[str, None]:
        """ The name of the stream. `None` if no name was given. """
        return self._name

    def set(self, read_stream: ReadStream):
        logger.debug("Setting the read stream {} to the loop stream {}".format(
            read_stream.name, self._name))
        self._py_loop_stream.set(read_stream._py_read_stream)


class IngestStream(object):
    """An :py:class:`IngestStream` enables drivers to inject data into a
    running ERDOS application.

    The driver can initialize a new :py:class:`IngestStream` and connect it to
    an :py:class:`Operator` through :py:func:`connect`. Similar to a
    :py:class:`WriteStream`, an :py:class:`IngestStream` provides a
    :py:func:`IngestStream.send` to enable the driver to send data to the
    operator to which it was connected.
    """
    def __init__(self, _name: Union[str, None] = None):
        self._py_ingest_stream = PyIngestStream(0, _name)
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

        internal_msg = _to_py_message(msg)
        self._py_ingest_stream.send(internal_msg)


class ExtractStream(object):
    """An :py:class:`ExtractStream` enables drivers to read data from a running
    ERDOS applications.

    The driver can initialize a new :py:class:`ExtractStream` by passing the
    instance of :py:class:`ReadStream` returned by :py:func:`connect`. Similar
    to a :py:class:`ReadStream`, an :py:class:`ExtractStream` provides
    :py:func:`ExtractStream.read` and :py:func:`ExtractStream.try_read` for
    reading data published on the corresponding `read_stream`.

    Args:
        read_stream (:py:class:`ReadStream`): The stream from which to
            read messages.
    """
    def __init__(self,
                 read_stream: ReadStream,
                 _name: Union[str, None] = None):
        if not isinstance(read_stream, ReadStream):
            raise ValueError(
                "ExtractStream needs to be initialized with a ReadStream. "
                "Received a {}".format(type(read_stream)))
        self._py_extract_stream = PyExtractStream(
            read_stream._py_read_stream,
            _name,
        )

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
