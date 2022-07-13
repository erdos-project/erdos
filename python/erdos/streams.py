from __future__ import annotations

import logging
import pickle
import uuid
from abc import ABC
from itertools import zip_longest
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from erdos.internal import (
    PyExtractStream,
    PyIngestStream,
    PyLoopStream,
    PyMessage,
    PyOperatorStream,
    PyReadStream,
    PyStream,
    PyWriteStream,
)
from erdos.message import Message, WatermarkMessage
from erdos.timestamp import Timestamp

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


def _parse_message(internal_msg: PyMessage) -> Message[T] | WatermarkMessage:
    """Creates a Message from an internal stream's response.

    Args:
        internal_msg: The internal message to parse.
    """
    if internal_msg.is_timestamped_data():
        assert (
            internal_msg.data is not None
        ), "Timestamped data message should always have data."
        timestamp = Timestamp(_py_timestamp=internal_msg.timestamp)
        data = pickle.loads(internal_msg.data)
        return Message(timestamp, data)
    if internal_msg.is_watermark():
        return WatermarkMessage(Timestamp(_py_timestamp=internal_msg.timestamp))
    raise Exception("Unable to parse message")


class Stream(ABC, Generic[T]):
    """Base class representing a stream to operators can be connected.
    from which is subclassed by streams that are used to
    connect operators in the driver.

    Note:
        This class should never be initialized manually.
    """

    def __init__(self, internal_stream: PyStream) -> None:
        self._internal_stream: PyStream = internal_stream

    @property
    def id(self) -> uuid.UUID:
        """The id of the stream."""
        return uuid.UUID(self._internal_stream.id())

    @property
    def name(self) -> str:
        """The name of the stream. The stream ID if none was given."""
        return self._internal_stream.name()

    @name.setter
    def name(self, name: str) -> None:
        self._internal_stream.set_name(name)

    def map(self, function: Callable[[T], U]) -> "OperatorStream[U]":
        """Applies the given function to each value sent on the stream, and outputs the
        results on the returned stream.

        Args:
            function: The function applied to each value sent on this stream.

        Returns:
            A stream that carries the results of the applied function.
        """

        def map_fn(serialized_data: bytes) -> bytes:
            result = function(pickle.loads(serialized_data))
            return pickle.dumps(result)

        return OperatorStream(self._internal_stream._map(map_fn))

    def flat_map(self, function: Callable[[T], Sequence[U]]) -> "OperatorStream[U]":
        """Applies the given function to each value sent on the stream, and outputs the
        sequence of received outputs as individual messages.

        Args:
            function: The function applied to each value sent on this stream.

        Returns:
            A stream that carries the results of the applied function.
        """

        # TODO (Sukrit): This method generates all the elements together and then sends
        # the messages out to downstream operators. Instead, the method should `yield`
        # individual elements so that they can be eagerly sent out.
        def flat_map_fn(serialized_data: bytes) -> Sequence[bytes]:
            mapped_values = function(pickle.loads(serialized_data))
            result = []
            for element in mapped_values:
                result.append(pickle.dumps(element))
            return result

        return OperatorStream(self._internal_stream._flat_map(flat_map_fn))

    def filter(self, function: Callable[[T], bool]) -> "OperatorStream[T]":
        """Applies the given function to each value sent on the stream, and sends the
        value on the returned stream if the function evaluates to `True`.

        Args:
            function: The function applied to each value sent on this stream. The value
                is retained if the function returns `True`.

        Returns:
            An stream that carries the filtered results from the applied function.
        """

        def filter_fn(serialized_data: bytes) -> bool:
            return function(pickle.loads(serialized_data))

        return OperatorStream(self._internal_stream._filter(filter_fn))

    def split(
        self, function: Callable[[T], bool]
    ) -> Tuple["OperatorStream[T]", "OperatorStream[T]"]:
        """Applies the given function to each value sent on the stream, and outputs the
        value to either the left or the right stream depending on if the returned
        boolean value is `True` or `False` respectively.

        Args:
            function: The function applied to each message sent on this stream.

        Returns:
            The left and the right stream respectively, containing the values output
            according to the split function.
        """

        def split_fn(serialized_data: bytes) -> bool:
            return function(pickle.loads(serialized_data))

        left_stream, right_stream = self._internal_stream._split(split_fn)
        return (OperatorStream(left_stream), OperatorStream(right_stream))

    def split_by_type(self, *data_type: Type[Any]) -> Tuple["OperatorStream[Any]", ...]:
        """Returns a stream for each provided type on which each message's data is an
        instance of that provided type.

        Message with data not corresponding to a provided type are filtered out.
        Useful for building operators that send messages with more than 2 data types.

        Args:
            data_type: the type of the data to be forwarded to the corresponding
                stream.

        Returns:
            A stream for each provided type where each message's data is an instance of
            that type.
        """
        # TODO(peter): optimize the implementation by moving logic to Rust.
        if len(data_type) == 0:
            raise ValueError("Did not receive a list of types.")

        last_stream = self
        streams = []
        for t in data_type[:-1]:
            s, last_stream = last_stream.split(lambda x: isinstance(x, t))
            streams.append(s)

        last_type = data_type[-1]
        last_stream = last_stream.filter(lambda x: isinstance(x, last_type))
        streams.append(last_stream)

        return tuple(streams)

    def timestamp_join(self, other: "Stream[U]") -> "OperatorStream[Tuple[T,U]]":
        """Joins the data with matching timestamps from the two different streams.

        Args:
            other: The stream to join with.

        Returns:
            A stream that carries the joined results from the two
            streams.
        """

        def join_fn(serialized_data_left: bytes, serialized_data_right: bytes) -> bytes:
            left_data = pickle.loads(serialized_data_left)
            right_data = pickle.loads(serialized_data_right)
            return pickle.dumps((left_data, right_data))

        return OperatorStream(
            self._internal_stream._timestamp_join(other._internal_stream, join_fn)
        )

    def concat(self, *other: "Stream[T]") -> "OperatorStream[T]":
        """Merges the data messages from the given streams into a single stream and
        forwards a watermark when a minimum watermark on the streams is achieved.

        Args:
            other: The other stream(s) to merge with.

        Returns:
            A stream that carries messages from all merged streams.
        """

        def merge_streams(
            paired_streams: Iterable[Tuple["Stream[T]", "Stream[T]"]]
        ) -> Sequence["OperatorStream[T]"]:
            merged_streams: List["OperatorStream[T]"] = []
            for left_stream, right_stream in paired_streams:
                if right_stream is not None:
                    merged_streams.append(
                        OperatorStream(
                            left_stream._internal_stream._concat(
                                right_stream._internal_stream
                            )
                        )
                    )
                else:
                    merged_streams.append(left_stream)
            return merged_streams

        if len(other) == 0:
            raise ValueError("Received empty list of streams to merge.")

        # Perform one merge step to remove dependency on `Stream[T]`.
        streams_to_be_merged = list(other) + [self]
        first_paired_streams = zip_longest(
            streams_to_be_merged[::2], streams_to_be_merged[1::2]
        )
        merged_streams = merge_streams(first_paired_streams)

        # Iteratively keep merging the streams in pairs of two.
        while len(merged_streams) != 1:
            paired_streams = zip_longest(merged_streams[::2], merged_streams[1::2])
            merged_streams = merge_streams(paired_streams)
        return merged_streams[0]


class ReadStream(Generic[T]):

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

    def __init__(self, _py_read_stream: PyReadStream) -> None:
        logger.debug(
            "Initializing ReadStream with the name: {}, and ID: {}.".format(
                _py_read_stream.name(), _py_read_stream.id
            )
        )
        self._py_read_stream: PyReadStream = _py_read_stream

    @property
    def name(self) -> str:
        """The name of the stream. A string version of the stream's ID if no
        name was given."""
        return self._py_read_stream.name()

    @property
    def id(self) -> uuid.UUID:
        """The id of the ReadStream."""
        return uuid.UUID(self._py_read_stream.id())

    def is_closed(self) -> bool:
        """Whether a top watermark message has been received."""
        return self._py_read_stream.is_closed()

    def read(self) -> Message[T] | WatermarkMessage:
        """Blocks until a message is read from the stream."""
        return _parse_message(self._py_read_stream.read())

    def try_read(self) -> Optional[Message[T] | WatermarkMessage]:
        """Tries to read a mesage from the stream.

        Returns None if no messages are available at the moment.
        """
        internal_msg = self._py_read_stream.try_read()
        if internal_msg is None:
            return None
        return _parse_message(internal_msg)


class WriteStream(Generic[T]):
    """A :py:class:`WriteStream` allows an operator to send messages and
    watermarks to other operators that connect to the corresponding
    :py:class:`ReadStream`.

    Note:
        This class is created automatically when ERDOS initializes an operator,
        and should never be initialized manually.
    """

    def __init__(self, _py_write_stream: PyWriteStream) -> None:
        logger.debug(
            "Initializing WriteStream with the name: {}, and ID: {}.".format(
                _py_write_stream.name(), _py_write_stream.id
            )
        )
        self._py_write_stream: PyWriteStream = (
            PyWriteStream() if _py_write_stream is None else _py_write_stream
        )

    @property
    def name(self) -> str:
        """The name of the stream. A string version of the stream's ID if no
        name was given."""
        return self._py_write_stream.name()

    @property
    def id(self) -> uuid.UUID:
        """The id of the WriteStream."""
        return uuid.UUID(self._py_write_stream.id())

    def is_closed(self) -> bool:
        """Whether a top watermark message has been sent."""
        return self._py_write_stream.is_closed()

    def send(self, msg: Message[T]) -> None:
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
                "Exception on stream {} ({}): {}".format(self.name, self.id, e)
            ) from e


class OperatorStream(Stream[T]):
    """Returned when connecting an operator to the dataflow graph.

    Note:
        This class is created automatically by the `connect` functions, and
        should never be initialized manually.
    """

    def __init__(self, operator_stream: PyOperatorStream) -> None:
        super().__init__(operator_stream)


class LoopStream(Stream[T]):
    """Stream placeholder used to construct loops in the dataflow graph.

    Note:
        Must call `connect_loop` with a valid :py:class:`OperatorStream` to
        complete the loop.
    """

    def __init__(self) -> None:
        super().__init__(PyLoopStream())

    def connect_loop(self, stream: OperatorStream[T]) -> None:
        if not isinstance(stream, OperatorStream) or not isinstance(
            stream._internal_stream, PyOperatorStream
        ):
            raise TypeError("Loop must be connected to an `OperatorStream`")
        if not isinstance(self._internal_stream, PyLoopStream):
            raise TypeError("Type mismatch on the internal LoopStream.")
        self._internal_stream.connect_loop(stream._internal_stream)


class IngestStream(Stream[T]):
    """An :py:class:`IngestStream` enables drivers to inject data into a
    running ERDOS application.

    The driver can initialize a new :py:class:`IngestStream` and connect it to
    an operator through the :code:`connect` family of functions. Similar to a
    :py:class:`WriteStream`, an :py:class:`IngestStream` provides a
    :py:func:`IngestStream.send` to enable the driver to send data to the
    operator to which it was connected.
    """

    def __init__(self, name: Union[str, None] = None) -> None:
        super().__init__(PyIngestStream(name))

    def is_closed(self) -> bool:
        """Whether the stream is closed.

        Returns True if the a top watermark message was sent or the
        IngestStream was unable to successfully set up.
        """
        if not isinstance(self._internal_stream, PyIngestStream):
            raise ValueError("Type mismatch on the internal IngestStream.")
        return self._internal_stream.is_closed()

    def send(self, msg: Message[T]) -> None:
        """Sends a message on the stream.

        Args:
            msg: the message to send. This may be a
                :py:class:`WatermarkMessage` or a :py:class:`Message`.
        """
        if not isinstance(msg, Message):
            raise TypeError("msg must inherent from erdos.Message!")
        if not isinstance(self._internal_stream, PyIngestStream):
            raise ValueError("Type mismatch on the internal IngestStream.")

        logger.debug(
            "Sending message {} on the Ingest stream {}".format(msg, self.name)
        )

        internal_msg = msg._to_py_message()
        self._internal_stream.send(internal_msg)


class ExtractStream(Generic[T]):
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

    def __init__(self, stream: OperatorStream[T]) -> None:
        if not isinstance(stream, OperatorStream) or not isinstance(
            stream._internal_stream, PyOperatorStream
        ):
            raise ValueError(
                "ExtractStream needs to be initialized with a Stream. "
                "Received a {}".format(type(stream))
            )
        self._py_extract_stream: PyExtractStream = PyExtractStream(
            stream._internal_stream
        )

    @property
    def name(self) -> str:
        """The name of the stream. The stream ID if no name was given."""
        return self._py_extract_stream.name()

    @property
    def id(self) -> uuid.UUID:
        """The id of the ExtractStream."""
        return uuid.UUID(self._py_extract_stream.id())

    def is_closed(self) -> bool:
        """Whether the stream is closed.

        Returns True if the a top watermark message was sent or the
        :py:class:`ExtractStream` was unable to successfully set up.
        """
        return self._py_extract_stream.is_closed()

    def read(self) -> Message[T] | WatermarkMessage:
        """Blocks until a message is read from the stream."""
        return _parse_message(self._py_extract_stream.read())

    def try_read(self) -> Optional[Message[T] | WatermarkMessage]:
        """Tries to read a mesage from the stream.

        Returns :code:`None` if no messages are available at the moment.
        """
        internal_msg = self._py_extract_stream.try_read()
        if internal_msg is None:
            return None
        return _parse_message(internal_msg)
