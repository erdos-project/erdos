import json
from collections import defaultdict, deque

import numpy as np

MAX_NUM_RUNTIME_SAMPLES = 1000


class Operator(object):
    """Operator abstract base class.

    Inherit from this class when creating an operator.
    """
    def __init__(self, *streams):
        """Instantiates the operator.

        ERDOS will pass read streams followed by write streams as arguments,
        matching the read streams and write streams in connect().

        Invoked automatically during erdos.run().

        ERDOS operators never need to call super().__init__() because
        setup is handled by the ERDOS backend code.
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
        """Connects the operator to its read streams and returns its write streams.

        This method should return all write streams it intends to use.

        Invoked automatically during `erdos.connect`.
        """
        raise NotImplementedError

    def run(self):
        """Runs the operator.

        Invoked automaticaly during `erdos.run()`.
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
    """Configuration details required by ERDOS Operators.
    """
    def __init__(self,
                 name=None,
                 flow_watermarks=True,
                 log_file_name=None,
                 csv_log_file_name=None,
                 profile_file_name=None):
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
