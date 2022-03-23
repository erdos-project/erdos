import logging
import pickle
import uuid
from abc import ABC
from typing import Union

from erdos.internal import (
    PyExtractStream,
    PyIngestStream,
    PyLoopStream,
    PyOperatorStream,
    PyReadStream,
    PyStream,
    PyWriteStream,
)
from erdos.message import Message, WatermarkMessage
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
        return WatermarkMessage(Timestamp(_py_timestamp=internal_msg.timestamp))
    raise Exception("Unable to parse message")


class Stream(ABC):
    """Base class representing a stream to operators can be connected.
    from which is subclassed by streams that are used to
    connect operators in the driver.

    Note:
        This class should never be initialized manually.
    """

    def __init__(self, internal_stream: PyStream):
        self._internal_stream = internal_stream

    @property
    def id(self) -> str:
        """The id of the stream."""
        return uuid.UUID(self._internal_stream.id())

    @property
    def name(self) -> str:
        """The name of the stream. The stream ID if none was given."""
        return self._internal_stream.name()

    @name.setter
    def name(self, name: str):
        self._internal_stream.set_name(name)


class ReadStream:
    """A :py:class:`ReadStream` allows an operator to read and do work on
    data sent by other operators on a corresponding :py:class:`WriteStream`.

    An operator that takes control of its execution using the :code:`run`
    method can retrieve the messages on a :py:class:`ReadStream` using the
    :py:meth:`ReadStream.read` or :py:meth:`ReadStream.try_read` methods.

    Note:
        This class is created automatically during :code:`run`, and
        should never be initialized manually.
        No callbacks are invoked if an operator takes control of the execution
        in :code:`run`.
    """

    def __init__(self, _py_read_stream: PyReadStream):
        logger.debug(
            "Initializing ReadStream with the name: {}, and ID: {}.".format(
                _py_read_stream.name(), _py_read_stream.id
            )
        )
        self._py_read_stream = _py_read_stream

    @property
    def name(self) -> str:
        """The name of the stream. A string version of the stream's ID if no
        name was given."""
        return self._py_read_stream.name()

    @property
    def id(self) -> str:
        """The id of the ReadStream."""
        return uuid.UUID(self._py_read_stream.id())

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


class WriteStream:
    """A :py:class:`WriteStream` allows an operator to send messages and
    watermarks to other operators that connect to the corresponding
    :py:class:`ReadStream`.

    Note:
        This class is created automatically when ERDOS initializes an operator,
        and should never be initialized manually.
    """

    def __init__(self, _py_write_stream: PyWriteStream):
        logger.debug(
            "Initializing WriteStream with the name: {}, and ID: {}.".format(
                _py_write_stream.name(), _py_write_stream.id
            )
        )
        self._py_write_stream = (
            PyWriteStream() if _py_write_stream is None else _py_write_stream
        )

    @property
    def name(self) -> str:
        """The name of the stream. A string version of the stream's ID if no
        name was given."""
        return self._py_write_stream.name()

    @property
    def id(self) -> str:
        """The id of the WriteStream."""
        return uuid.UUID(self._py_write_stream.id())

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
        logger.debug("Sending message {} on the stream {}".format(msg, self.name))

        # Raise exception with the name.
        try:
            return self._py_write_stream.send(internal_msg)
        except Exception as e:
            raise Exception(
                "Exception on stream {} ({})".format(self.name, self.id)
            ) from e


class OperatorStream(Stream):
    """Returned when connecting an operator to the dataflow graph.

    Note:
        This class is created automatically by the `connect` functions, and
        should never be initialized manually.
    """

    def __init__(self, operator_stream: PyOperatorStream):
        super().__init__(operator_stream)


class LoopStream(Stream):
    """Stream placeholder used to construct loops in the dataflow graph.

    Note:
        Must call `connect_loop` with a valid :py:class:`OperatorStream` to
        complete the loop.
    """

    def __init__(self):
        super().__init__(PyLoopStream())

    def connect_loop(self, stream: OperatorStream):
        if not isinstance(stream, OperatorStream):
            raise TypeError("Loop must be connected to an `OperatorStream`")
        self._internal_stream.connect_loop(stream._internal_stream)


class IngestStream(Stream):
    """An :py:class:`IngestStream` enables drivers to inject data into a
    running ERDOS application.

    The driver can initialize a new :py:class:`IngestStream` and connect it to
    an operator through the :code:`connect` family of functions. Similar to a
    :py:class:`WriteStream`, an :py:class:`IngestStream` provides a
    :py:func:`IngestStream.send` to enable the driver to send data to the
    operator to which it was connected.
    """

    def __init__(self, name: Union[str, None] = None):
        super().__init__(PyIngestStream(name))

    def is_closed(self) -> bool:
        """Whether the stream is closed.

        Returns True if the a top watermark message was sent or the
        IngestStream was unable to successfully set up.
        """
        return self._internal_stream.is_closed()

    def send(self, msg: Message):
        """Sends a message on the stream.

        Args:
            msg: the message to send. This may be a
                :py:class:`WatermarkMessage` or a :py:class:`Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")

        logger.debug(
            "Sending message {} on the Ingest stream {}".format(msg, self.name)
        )

        internal_msg = msg._to_py_message()
        self._internal_stream.send(internal_msg)


class ExtractStream:
    """An :py:class:`ExtractStream` enables drivers to read data from a
    running ERDOS applications.

    The driver can initialize a new :py:class:`ExtractStream` by passing the
    instance of :py:class:`OperatorStream` returned by the :code:`connect`
    family of functions. Similar to a :py:class:`ReadStream`, an
    :py:class:`ExtractStream` provides :py:meth:`.read` and
    :py:meth:`.try_read` for reading data published on the corresponding
    :py:class:`OperatorStream`.

    Args:
        stream: The stream from which to read messages.
    """

    def __init__(self, stream: OperatorStream):
        if not isinstance(stream, OperatorStream):
            raise ValueError(
                "ExtractStream needs to be initialized with a Stream. "
                "Received a {}".format(type(stream))
            )
        self._py_extract_stream = PyExtractStream(stream._internal_stream)

    @property
    def name(self) -> str:
        """The name of the stream. The stream ID if no name was given."""
        return self._py_extract_stream.name()

    @property
    def id(self) -> str:
        """The id of the ExtractStream."""
        return uuid.UUID(self._py_extract_stream.id())

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

        Returns :code:`None` if no messages are available at the moment.
        """
        internal_msg = self._py_extract_stream.try_read()
        if internal_msg is None:
            return None
        return _parse_message(internal_msg)
