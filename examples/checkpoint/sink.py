from copy import copy
from erdos.op import Op
from erdos.utils import setup_logging
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.timestamp import Timestamp
from collections import deque
import checkpoint_util


class Sink(Op):
    def __init__(self,
                 name,
                 checkpoint_enable=True,
                 checkpoint_freq=10,
                 state_size=10,
                 log_file_name=None):
        super(Sink, self).__init__(name,
                                   checkpoint_enable=checkpoint_enable,
                                   checkpoint_freq=checkpoint_freq)
        self._logger = setup_logging(self.name, log_file_name)
        self._state_size = state_size
        self._state = deque()
        self._last_received_num = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(checkpoint_util.is_control_stream) \
            .add_callback(Sink.on_rollback_msg)
        input_streams.filter(checkpoint_util.is_not_control_stream) \
            .add_callback(Sink.on_msg)
        return [DataStream(name="sink_snapshot", labels={'no_watermark': 'true'})]

    def on_msg(self, msg):
        seq_num = int(msg.data)
        # Check duplicate
        if self._last_received_num is None:
            self._last_received_num = seq_num
        elif self._last_received_num + 1 == seq_num:
            self._logger.info('received %d' % seq_num)
            self._last_received_num = seq_num
        else:   # sink receives duplicates
            self._logger.info('received DUPLICATE or WRONG-ORDER message %d' % seq_num)
        # Build state
        if len(self._state) == self._state_size:  # state is full
            self._state.popleft()
        self._state.append(seq_num)

    def checkpoint_condition(self, timestamp):
        if timestamp.coordinates[0] % self._checkpoint_freq == 0:
            return True
        return False

    def checkpoint(self, timestamp):
        # Send snapshot timestamp (latest received seq num) to controller
        self.get_output_stream("sink_snapshot").send(Message(0, timestamp))
        return copy(self._state)

    def restore(self, timestamp, state):
        if timestamp is None:
            # Rollback to beginning
            self._state = deque()
        else:
            self._state = state

    def on_rollback_msg(self, msg):
        if msg.data == checkpoint_util.CheckpointControllerCommand.ROLLBACK:
            # XXX(ionel): This method should be invoked by the system.
            state = self._rollback(msg.timestamp)

    def execute(self):
        self.spin()
