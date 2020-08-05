import json
from collections import defaultdict, deque

import numpy as np

MAX_NUM_RUNTIME_SAMPLES = 1000


class Operator(object):
    """An :py:class:`Operator` is an abstract base class that needs to be
    inherited by user-defined operators in order to be run in an ERDOS dataflow
    graph.

    A user-defined operator needs to inherit from :py:class:`Operator` and
    implement :py:func:`Operator.__init__` and :py:func:`Operator.connect` in
    order to be connected to the dataflow graph. For example, a `MapOperator`
    that takes in a single input stream and outputs data on a single output
    stream can be implemented as follows::

        class MapOperator(erdos.Operator):
            def __init__(self, read_stream, write_stream):
                # Register callbacks on read streams and save write streams.
                pass

            @staticmethod
            def connect(read_stream):
                return erdos.WriteStream()

    Instead of ERDOS invoking callbacks registered on the read streams, an
    operator can also take control of the execution by overriding the
    :py:func:`Operator.run` method as follows::

        class MapOperator(erdos.Operator):
            def run(self):
                message = self.read_stream.read()
                # Work on the message.
    """
    def __init__(self, *streams):
        """Instantiates the operator.

        ERDOS will pass read streams followed by write streams as arguments,
        matching the read streams and write streams in
        :py:func:`Operator.connect`.

        Invoked automatically during :py:func:`.run`.

        Note:
            An ERDOS operator implementation should not call
            `super().__init__()` because the setup is handled by ERDOS.
        """
        pass

    def __new__(cls, *args, **kwargs):
        """Set up variables before call to `__init__` on the python end.

        More setup is done in the Rust backend at `src/python/mod.rs`.
        """
        instance = super(Operator, cls).__new__(cls, *args, **kwargs)
        instance._trace_events = []
        instance._runtime_stats = defaultdict(deque)
        return instance

    @staticmethod
    def connect(*read_streams):
        """Connects the operator to its read streams and returns its
        write streams. This method should return all the write streams that the
        operator intends to use.

        Invoked automatically during :py:func:`.connect`.
        """
        raise NotImplementedError

    def run(self):
        """Runs the operator.

        Invoked automatically during :py:func:`.run`.
        """
        pass

    def destroy(self):
        """Destroys the operator.

        Invoked automatically once all `ReadStreams` the operator reads from
        are closed and `run()` completes.
        """
        pass

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


class OperatorConfig(object):
    """ An :py:class:`OperatorConfig` allows developers to configure an
    :py:class:`Operator`.

    An :py:class:`Operator` can query the configuration passed to it by the
    driver by accessing the properties in `self.config`. The below example
    shows how a `LoggerOperator` can access the log file name passed to the
    operator by the driver::

        class LoggerOperator(erdos.Operator):
            def __init__(self, input_stream):
                # Set up a logger.
                _log = self.config.log_file_name
                self.logger = erdos.utils.setup_logging(self.config.name, _log)
    """
    def __init__(self,
                 name: str = None,
                 flow_watermarks: bool = True,
                 log_file_name: str = None,
                 csv_log_file_name: str = None,
                 profile_file_name: str = None):
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
