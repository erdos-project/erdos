import logging
from collections import deque
from time import sleep


class Op(object):
    """Operator base class.

    All operators must inherit from this class, and must implement:

    1. setup_streams: Constructs the object's output data streams from the
    input data streams and arguments.

    Operators may optionally implement:

    2. __init__: Sets up operator state.
    3. execute: Invoked upon operator execution.

    Attributes:
        name (str): A unique string naming the operator.
        input_streams (list of DataStream): data streams to which the operator
            is subscribed.
        output_streams (dict of str -> DataStream): Data streams on which the
            operator publishes. Mapping between name and data stream.
        freq_actor: A Ray actor used for periodic tasks.
    """

    def __init__(self, name, checkpoint_enable=False, checkpoint_freq=None):
        self.name = name
        self.input_streams = []
        self.output_streams = {}
        self.freq_actor = None
        self.progress_tracker = None
        self.framework = None
        self._stream_to_high_watermark = {}
        self._stream_ignore_watermarks = set()  # input streams that do not send watermarks

        # Checkpoint variables
        self._checkpoint_enable = checkpoint_enable
        self._checkpoint_freq = checkpoint_freq
        if self._checkpoint_enable:
            assert self._checkpoint_freq is not None
        self._checkpoints = deque()

    def get_output_stream(self, name):
        """Returns the output stream matching name"""
        return self.output_streams[name]

    def notify_at(self, timestamp):
        """Subscribes the operator to receive a notification."""
        self.progress_tracker.notify_at.remote(self.name, timestamp)

    def on_notify(self, timestamp):
        """Called after a timestamp completes"""
        pass

    def execute(self):
        """Invoked upon operator execution.

        User override. Otherwise, spin.
        """
        self.spin()

    @staticmethod
    def setup_streams(input_streams, **kwargs):
        """Subscribes to input data streams and constructs output data streams.

        Required user override.

        Args:
            input_streams (DataStreams): data streams from connected upstream
                operators which the operator may subscribe to.
            kwargs: Arbitrary keyword arguments used to subscribe to input
                data streams or construct output data streams.

        Returns:
            (list of DataStream): output data streams on which the operator
            publishes.
        """
        raise NotImplementedError(
            "User must define setup_streams in operators.")

    def spin(self):
        """Abstracts framework specific spin methods."""
        if self.framework == "ros":
            import rospy
            rospy.spin()
        elif self.framework == "local":
            while True:
                sleep(0.5)
        elif self.framework == "ray":
            pass
        else:
            logging.critical("Unexpected framework %s", self.framework)

    def log_event(self, processing_time, timestamp, log_message=None):
        pass

    def checkpoint_condition(self, timestamp):
        """
        User Override: if holds True, checkpoint function will be invoked
        :param timestamp: watermark timestamp
        """
        if timestamp.coordinates[0] % self._checkpoint_freq == 0:
            return True
        return False

    def checkpoint(self, timestamp):
        """ Provided by the user to checkpoint state.

        Required user override if checkpoint enabled.
        Return a checkpoint id and a checkpoint state
        """
        return None

    def restore(self, timestamp, state):
        """ Provided by the user to restore state from a checkpoint.

        Required user override.

        Args:
             timestamp: the timestamp at which which checkpoint_condition
                 evaluated to True.
        """
        pass

    def _rollback(self, timestamp):
        state = None
        checkpoint_timestamp = None
        while (len(self._checkpoints) > 0):
            (checkpoint_timestamp, state) = self._checkpoints.pop()
            if checkpoint_timestamp <= timestamp:
                break
        assert (checkpoint_timestamp is None or
                checkpoint_timestamp == timestamp), 'Could not find matching checkpoint'
        self._reset_watermarks(timestamp)
        self.restore(timestamp, state)

    def _garbage_collect_checkpoints(self, timestamp):
        """ Remove all checkpoints that have timestamp less than or equal to
        timestamp.
        """
        while (len(self._checkpoints) > 0 and self._checkpoints[0] <= timestamp):
            self._checkpoints.popleft()

    def _checkpoint(self, timestamp):
        assert (len(self._checkpoints) == 0 or
                self._checkpoints[-1][0] < timestamp), 'Checkpoint is stale'
        state = self.checkpoint(timestamp)
        self._checkpoints.append((timestamp, state))

    def _reset_watermarks(self, timestamp):
        """ Reset the progress (watermark) of the operator """
        for input_stream in self.input_streams:
            if input_stream.name in self._stream_to_high_watermark:
                self._stream_to_high_watermark[input_stream.name] = timestamp

    def _add_input_streams(self, input_streams):
        """Setups and updates all input streams."""
        self.input_streams = self.input_streams + input_streams

    def _add_output_streams(self, output_streams):
        """Updates the dictionary of output data streams."""
        for output_stream in output_streams:
            self.output_streams[output_stream.name] = output_stream

    def _internal_setup_streams(self):
        """Setups input and output streams."""
        # Set up output streams before input streams.
        # This prevents errors where the operator recieves a message, executes
        # the callback, and sends a message before output streams are set up.
        for output_stream in self.output_streams.values():
            output_stream.setup()
        for input_stream in self.input_streams:
            if input_stream.labels.get('no_watermark', 'false') == 'true':
                self._stream_ignore_watermarks.add(input_stream.name)
            input_stream.setup()
