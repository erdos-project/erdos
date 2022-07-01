import json
from collections import defaultdict, deque
from typing import Any

import numpy as np

from erdos.context import (
    OneInOneOutContext,
    OneInTwoOutContext,
    SinkContext,
    TwoInOneOutContext,
)
from erdos.streams import ReadStream, WriteStream

MAX_NUM_RUNTIME_SAMPLES = 1000


class BaseOperator:
    """A :py:class:`BaseOperator` is an internal class that provides the
    methods common to the individual operators.
    """

    @property
    def id(self):
        """Returns the operator's ID."""
        return self._id

    @property
    def config(self):
        """Returns the operator's config."""
        return self._config

    def add_trace_event(self, event):
        """Records a profile trace event."""
        self._trace_events.append(event)
        self._trace_event_logger.info(json.dumps(event))
        event_name = event["name"]
        self._runtime_stats[event_name].append(event["dur"])
        if len(self._runtime_stats[event_name]) > MAX_NUM_RUNTIME_SAMPLES:
            self._runtime_stats[event_name].popleft()

    def get_runtime(self, event_name, percentile):
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
            return np.percentile(self._runtime_stats[event_name], percentile)

    def save_trace_events(self, file_name):
        import json

        with open(file_name, "w") as write_file:
            json.dump(self._trace_events, write_file)


class Source(BaseOperator):
    """A :py:class:`Source` is an abstract base class that needs to be
    inherited by user-defined source operators that generate data on a single
    :py:class:`.WriteStream` in an ERDOS dataflow graph.

    A user-defined operator needs to implement the :py:func:`Source.run` and
    :py:func:`Source.destroy` in order to take control of the execution and the
    teardown of the operator respectively.
    """

    def __new__(cls, *args, **kwargs):
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(Source, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(self, write_stream: WriteStream):
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with a
        :py:class:`.WriteStream` to send data on.

        Args:
            write_stream: A :py:class:`.WriteStream` instance to send data on.
        """

    def destroy(self):
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, and can be used by the operator to teardown its state
        gracefully.
        """


class Sink(BaseOperator):
    """A :py:class:`Sink` is an abstract class that needs to be inherited by
    user-defined sink operators that consume data from a single
    :py:class:`.ReadStream` in an ERDOS dataflow graph.

    The user-defined operator can either implement the :py:func:`run` method
    and retrieve data from the provided `read_stream` or implement the
    :py:func:`on_data` and :py:func:`on_watermark` methods to request a
    callback upon receipt of messages and watermarks.
    """

    def __new__(cls, *args, **kwargs):
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(Sink, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(self, read_stream: ReadStream):
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with a
        :py:class:`.ReadStream` to retrieve data from.

        Args:
            read_stream: A :py:class:`.ReadStream` instance to read data from.
        """

    def on_data(self, context: SinkContext, data: Any):
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.SinkContext` instance to retrieve metadata
                about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: SinkContext):
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage` on
        the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.SinkContext` instance to retrieve metadata
                about the current invocation of the callback.
        """

    def destroy(self):
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on the
        read stream, and can be used by the operator to teardown its state
        gracefully.
        """


class OneInOneOut(BaseOperator):
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

    def __new__(cls, *args, **kwargs):
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(OneInOneOut, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(self, read_stream: ReadStream, write_stream: WriteStream):
        """Runs the operator.

        Invoked automatically by ERDOS, and provided with a
        :py:class:`.ReadStream` to retrieve data from, and a
        :py:class:`.WriteStream` to send data on.

        Args:
            read_stream: A :py:class:`.ReadStream` instance to read data from.
            write_stream: A :py:class:`.WriteStream` instance to send data on.
        """

    def on_data(self, context: OneInOneOutContext, data: Any):
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.OneInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: OneInOneOutContext):
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage` on
        the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.OneInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
        """

    def destroy(self):
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on the
        read stream, and can be used by the operator to teardown its state
        gracefully.
        """


class TwoInOneOut(BaseOperator):
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

    def __new__(cls, *args, **kwargs):
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(TwoInOneOut, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(
        self,
        left_read_stream: ReadStream,
        right_read_stream: ReadStream,
        write_stream: WriteStream,
    ):
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

    def on_left_data(self, context: TwoInOneOutContext, data: Any):
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        `left_read_stream`.

        Args:
            context: A :py:class:`.TwoInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_right_data(self, context: TwoInOneOutContext, data: Any):
        """Callback invoked puon receipt of a :py:class:`.Message` on the
        `right_read_stream`.

        Args:
            context: A :py:class:`.TwoInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: TwoInOneOutContext):
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage`
        across the two instances of the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.TwoInOneOutContext` instance to retrieve
                metadata about the current invocation of the callback.
        """

    def destroy(self):
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on
        both the read streams, and can be used by the operator to teardown its
        state gracefully.
        """


class OneInTwoOut(BaseOperator):
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

    def __new__(cls, *args, **kwargs):
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(OneInTwoOut, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    def run(
        self,
        read_stream: ReadStream,
        left_write_stream: WriteStream,
        right_write_stream: WriteStream,
    ):
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

    def on_data(self, context: OneInTwoOutContext, data: Any):
        """Callback invoked upon receipt of a :py:class:`.Message` on the
        `read_stream`.

        Args:
            context: A :py:class:`.OneInTwoOutContext` instance to retrieve
                metadata about the current invocation of the callback.
            data: The data contained in the message received on the read
                stream.
        """

    def on_watermark(self, context: OneInTwoOutContext):
        """Callback invoked upon receipt of a :py:class:`.WatermarkMessage` on
        the operator's :py:class:`.ReadStream`.

        Args:
            context: A :py:class:`.OneInTwoOutContext` instance to retrieve
                metadata about the current invocation of the callback.
        """

    def destroy(self):
        """Destroys the operator.

        Invoked automatically by ERDOS once :py:func:`.run` finishes its
        execution, or when a watermark for the top timestamp is received on
        the read stream, and can be used by the operator to teardown its state
        gracefully.
        """


class OperatorConfig:
    """An :py:class:`OperatorConfig` allows developers to configure an
    operator.

    An operator` can query the configuration passed to it by the driver by
    accessing the properties in :code:`self.config`. The below example shows
    how a `LoggerOperator` can access the log file name passed to the operator
    by the driver::

        class LoggerOperator(erdos.Operator):
            def __init__(self, input_stream):
                # Set up a logger.
                _log = self.config.log_file_name
                self.logger = erdos.utils.setup_logging(self.config.name, _log)
    """

    def __init__(
        self,
        name: str = None,
        flow_watermarks: bool = True,
        log_file_name: str = None,
        csv_log_file_name: str = None,
        profile_file_name: str = None,
    ):
        self._name = name
        self._flow_watermarks = flow_watermarks
        self._log_file_name = log_file_name
        self._csv_log_file_name = csv_log_file_name
        self._profile_file_name = profile_file_name

    @property
    def name(self):
        """Name of the operator."""
        return self._name

    @property
    def flow_watermarks(self):
        """Whether to automatically pass on the low watermark."""
        return self._flow_watermarks

    @property
    def log_file_name(self):
        """File name used for logging."""
        return self._log_file_name

    @property
    def csv_log_file_name(self):
        """File name used for logging to CSV."""
        return self._csv_log_file_name

    @property
    def profile_file_name(self):
        """File named used for profiling an operator's performance."""
        return self._profile_file_name

    def __str__(self):
        return "OperatorConfig(name={}, flow_watermarks={})".format(
            self.name, self.flow_watermarks
        )

    def __repr__(self):
        return str(self)
