from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Generic, List, Optional, TypeVar

import numpy as np

from erdos.config import OperatorConfig
from erdos.context import (
    OneInOneOutContext,
    OneInTwoOutContext,
    SinkContext,
    TwoInOneOutContext,
)
from erdos.streams import ReadStream, WriteStream

MAX_NUM_RUNTIME_SAMPLES = 1000

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class BaseOperator:
    """A :py:class:`BaseOperator` is an internal class that provides the
    methods common to the individual operators.
    """

    _id: uuid.UUID
    _config: OperatorConfig
    _trace_events: List[Dict[str, Optional[str | int | Dict[str, str]]]]
    _trace_event_logger: logging.Logger
    _runtime_stats: Dict[str, Deque[Optional[str | int | Dict[str, str]]]]

    @property
    def id(self) -> uuid.UUID:
        """Returns the operator's ID."""
        return self._id

    @property
    def config(self) -> OperatorConfig:
        """Returns the operator's config."""
        return self._config

    def add_trace_event(
        self, event: Dict[str, Optional[str | int | Dict[str, str]]]
    ) -> None:
        """Records a profile trace event."""
        self._trace_events.append(event)
        self._trace_event_logger.info(json.dumps(event))
        event_name = event["name"]
        assert isinstance(event_name, str), "Event name was not a string."
        self._runtime_stats[event_name].append(event["dur"])
        if len(self._runtime_stats[event_name]) > MAX_NUM_RUNTIME_SAMPLES:
            self._runtime_stats[event_name].popleft()

    def get_runtime(self, event_name: str, percentile: int | float) -> Optional[float]:
        """Gets the runtime percentile for a given type of event.

        Args:
            event_name (str): The name of the event to get runtime for.
            percentile (int): The percentile runtime to get.

        Returns:
            (float): Runtime in microseconds, or None if the operator doesn't
            have any runtime stats for the given event name.
        """
        if event_name not in self._runtime_stats:
            # We don't have any runtime statistics.
            return None
        else:
            stats = []
            for stat in self._runtime_stats[event_name]:
                assert isinstance(
                    stat, (int, float)
                ), f"Non-numeric stat found in runtime_stats: {stat}"
                stats.append(stat)

            return float(np.percentile(stats, percentile))

    def save_trace_events(self, file_name: str) -> None:
        import json

        with open(file_name, "w") as write_file:
            json.dump(self._trace_events, write_file)


class Source(BaseOperator, Generic[T]):
    """A :py:class:`Source` is an abstract base class that needs to be
    inherited by user-defined source operators that generate data on a single
    :py:class:`.WriteStream` in an ERDOS dataflow graph.

    A user-defined operator needs to implement the :py:func:`Source.run` and
    :py:func:`Source.destroy` in order to take control of the execution and the
    teardown of the operator respectively.
    """

    def __new__(cls, *args: List[Any], **kwargs: Dict[str, Any]) -> Source[T]:
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(Source, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(self, write_stream: WriteStream[T]) -> None:
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with a
        :py:class:`.WriteStream` to send data on.

        Args:
            write_stream: A :py:class:`.WriteStream` instance to send data on.
        """

    def destroy(self) -> None:
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, and can be used by the operator to teardown its state
        gracefully.
        """


class Sink(BaseOperator, Generic[T]):
    """A :py:class:`Sink` is an abstract class that needs to be inherited by
    user-defined sink operators that consume data from a single
    :py:class:`.ReadStream` in an ERDOS dataflow graph.

    The user-defined operator can either implement the :py:func:`run` method
    and retrieve data from the provided `read_stream` or implement the
    :py:func:`on_data` and :py:func:`on_watermark` methods to request a
    callback upon receipt of messages and watermarks.
    """

    def __new__(cls, *args: List[Any], **kwargs: Dict[str, Any]) -> Sink[T]:
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(Sink, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(self, read_stream: ReadStream[T]) -> None:
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with a
        :py:class:`.ReadStream` to retrieve data from.

        Args:
            read_stream: A :py:class:`.ReadStream` instance to read data from.
        """

    def on_data(self, context: SinkContext, data: T) -> None:
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.SinkContext` instance to retrieve metadata
                about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: SinkContext) -> None:
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage` on
        the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.SinkContext` instance to retrieve metadata
                about the current invocation of the callback.
        """

    def destroy(self) -> None:
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on the
        read stream, and can be used by the operator to teardown its state
        gracefully.
        """


class OneInOneOut(BaseOperator, Generic[T, U]):
    """A :py:class:`OneInOneOut` is an abstract base class that needs to be
    inherited by user-defined operators that consume data from a single
    :py:class:`.ReadStream` and produce data on a single
    :py:class:`.WriteStream` in an ERDOS dataflow graph.

    The user-defined operator can either implement the :py:func:`run` method
    and retrieve data from the provided `read_stream` and send data on the
    `write_stream` or implement the :py:func:`on_data` and
    :py:func:`on_watermark` methods to request a callback upon receipt of
    messages and watermarks.
    """

    def __new__(cls, *args: List[Any], **kwargs: Dict[str, Any]) -> OneInOneOut[T, U]:
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(OneInOneOut, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(self, read_stream: ReadStream[T], write_stream: WriteStream[U]) -> None:
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with a
        :py:class:`.ReadStream` to retrieve data from, and a
        :py:class:`.WriteStream` to send data on.

        Args:
            read_stream: A :py:class:`.ReadStream` instance to read data from.
            write_stream: A :py:class:`.WriteStream` instance to send data on.
        """

    def on_data(self, context: OneInOneOutContext[U], data: T) -> None:
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.OneInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: OneInOneOutContext[U]) -> None:
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage` on
        the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.OneInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
        """

    def destroy(self) -> None:
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on the
        read stream, and can be used by the operator to teardown its state
        gracefully.
        """


class TwoInOneOut(BaseOperator, Generic[T, U, V]):
    """A :py:class:`TwoInOneOut` is an abstract base class that needs to be
    inherited by user-defined operators that consume data from two
    :py:class:`.ReadStream` instances and produces data on a single
    :py:class:`.WriteStream` in an ERDOS dataflow graph.

    The user-defined operator can either implement the :py:func:`run` method
    and retrieve data from the provided `left_read_stream` and
    `right_read_stream` or implement the :py:func:`on_left_data`,
    :py:func:`on_right_data` and :py:func:`on_watermark` methods to request a
    callback upon receipt of messages and watermarks.
    """

    def __new__(
        cls, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> TwoInOneOut[T, U, V]:
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(TwoInOneOut, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(
        self,
        left_read_stream: ReadStream[T],
        right_read_stream: ReadStream[U],
        write_stream: WriteStream[V],
    ) -> None:
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with two instances of
        :py:class:`.ReadStream` to retrieve data from, and a
        :py:class:`.WriteStream` to send data on.

        Args:
            left_read_stream: The first :py:class:`.ReadStream` instance to
                read data from.
            right_read_stream: The second :py:class:`.ReadStream` instance to
                read data from.
            write_stream: A :py:class:`.WriteStream` instance to send data on.
        """

    def on_left_data(self, context: TwoInOneOutContext[V], data: T) -> None:
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        `left_read_stream`.

        Args:
            context: A :py:class:`.TwoInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_right_data(self, context: TwoInOneOutContext[V], data: U) -> None:
        """Callback invoked puon receipt of a :py:class:`.Message` on the
        `right_read_stream`.

        Args:
            context: A :py:class:`.TwoInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: TwoInOneOutContext[V]) -> None:
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage`
        across the two instances of the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.TwoInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
        """

    def destroy(self) -> None:
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on
        both the read streams, and can be used by the operator to teardown its
        state gracefully.
        """


class OneInTwoOut(BaseOperator, Generic[T, U, V]):
    """A :py:class:`OneInTwoOut` is an abstract base class that needs to be
    inherited by user-defined operators that consume data from a single
    :py:class:`.ReadStream` instance and produce data on two instances of
    :py:class:`.WriteStream` in an ERDOS dataflow graph.

    The user-defined operator can either implement the :py:func:`run` method
    and retrieve data from the provided `read_stream` and produce data on the
    `left_write_stream` and `right_write_stream`, or implement the
    :py:func:`on_data` and :py:func:`on_watermark` methods to request a
    callback upon receipt of messages and watermarks.
    """

    def __new__(
        cls, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> OneInTwoOut[T, U, V]:
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(OneInTwoOut, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(
        self,
        read_stream: ReadStream[T],
        left_write_stream: WriteStream[U],
        right_write_stream: WriteStream[V],
    ) -> None:
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with a
        :py:class:`.ReadStream` instance to retrieve data from, and two
        :py:class:`.WriteStream` instances to send data on.

        Args:
            read_stream: The :py:class:`.ReadStream` instance to retrieve data
                from.
            left_write_stream: The first :py:class:`.WriteStream` instance to
                send data on.
            right_write_stream: The second :py:class:`.WriteStream` instance to
                send data on.
        """

    def on_data(self, context: OneInTwoOutContext[U, V], data: T) -> None:
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        `read_stream`.

        Args:
            context: A :py:class:`.OneInTwoOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: OneInTwoOutContext[U, V]) -> None:
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage` on
        the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.OneInTwoOutContext` instance to retrieve
                metadata about the current invocation of the callback.
        """

    def destroy(self) -> None:
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on
        the read stream, and can be used by the operator to teardown its state
        gracefully.
        """
