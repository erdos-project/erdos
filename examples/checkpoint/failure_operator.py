import checkpoint_util
from collections import deque
from copy import copy

from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging


class FailureOperator(Op):
    def __init__(self,
                 name,
                 checkpoint_enable=True,
                 checkpoint_freq=10,
                 state_size=10,
                 log_file_name=None):
        super(FailureOperator, self).__init__(name,
                                              checkpoint_enable=checkpoint_enable,
                                              checkpoint_freq=checkpoint_freq)
        self._logger = setup_logging(self.name, log_file_name)
        self._state_size = state_size
        self._state = deque()

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(checkpoint_util.is_control_stream)\
            .add_callback(FailureOperator.on_rollback_msg)
        input_streams.filter(checkpoint_util.is_not_control_stream)\
            .add_callback(FailureOperator.on_msg)
        return [DataStream(name="failure_op_out")]

    def on_msg(self, msg):
        # Build state
        if len(self._state) == self._state_size:  # state is full
            self._state.popleft()
        self._state.append(int(msg.data))

        # Send msg
        self.get_output_stream("failure_op_out").send(msg)

    def checkpoint_condition(self, timestamp):
        if timestamp.coordinates[0] % self._checkpoint_freq == 0:
            return True
        return False

    def checkpoint(self, timestamp):
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
