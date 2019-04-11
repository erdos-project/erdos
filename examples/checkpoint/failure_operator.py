from copy import copy
from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
import checkpoint_util
from collections import deque


class FailureOperator(Op):
    def __init__(self,
                 name,
                 checkpoint_freq=10,
                 state_size=10,
                 log_file_name=None):
        super(FailureOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._state_size = state_size
        self._state = deque()
        self._checkpoint_freq = checkpoint_freq
        self._checkpoints = dict()
        self._seq_num = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(checkpoint_util.is_control_stream)\
            .add_callback(FailureOperator.on_rollback_msg)
        input_streams.filter(checkpoint_util.is_not_control_stream)\
            .add_callback(FailureOperator.on_msg)
        return [DataStream(name="failure_op_out")]

    def on_msg(self, msg):
        if msg.stream_name == 'watermark':
            self.on_watermark(msg)
        else:
            self._seq_num = int(msg.data)
            # Build state
            if len(self._state) == self._state_size:  # state is full
                self._state.popleft()
            self._state.append(self._seq_num)

            # Send msg
            self.get_output_stream("failure_op_out").send(msg)

    def on_watermark(self, msg):
        # Deal with watermark and checkpoint
        if self._seq_num % self._checkpoint_freq == 0:
            # Checkpoint
            snapshot_id = self._state[-1]  # latest received seq num/timestamp
            assert snapshot_id not in self._checkpoints
            self._checkpoints[snapshot_id] = copy(self._state)

    def on_rollback_msg(self, msg):
        (control_msg, rollback_id) = msg.data
        if control_msg == checkpoint_util.CheckpointControllerCommand.ROLLBACK:
            if rollback_id is None:
                # Assume sink didn't process any messages, so start over
                self._seq_num = None
                self._state = deque()
                self._logger.info("%s rollback to START OVER" % self.name)
            else:
                rollback_id = int(rollback_id)
                self._seq_num = rollback_id + 1
                self._state = self._checkpoints[rollback_id]
                self._logger.info("%s rollback to SNAPSHOT ID %d" % (self.name, rollback_id))

    def execute(self):
        self.spin()
