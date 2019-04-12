from copy import copy
from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
from erdos.timestamp import Timestamp
import checkpoint_util
from collections import deque


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
        self._seq_num = int(msg.data)
        # Build state
        if len(self._state) == self._state_size:  # state is full
            self._state.popleft()
        self._state.append(self._seq_num)

        # Send msg
        self.get_output_stream("failure_op_out").send(msg)

    def checkpoint(self):
        # Override base class checkpoint function
        snapshot_id = self._state[-1]  # latest received seq num/timestamp
        assert snapshot_id not in self._checkpoints
        self._checkpoints[snapshot_id] = copy(self._state)
        self._logger.info('checkpointed at latest stored data %d' % snapshot_id)

    def on_rollback_msg(self, msg):
        (control_msg, rollback_id) = msg.data
        if control_msg == checkpoint_util.CheckpointControllerCommand.ROLLBACK:
            if rollback_id is None:
                # Sink did not snapshot anything
                self.rollback_to_beginning()
            else:
                rollback_id = int(rollback_id)
                # Find snapshot id that is the closest to and smaller/equal to the rollback_id
                ids = sorted(self._checkpoints.keys())
                rollback_id = [id for id in ids if id <= rollback_id][-1]

                # Reset watermark
                self.reset_progress(Timestamp(coordinates=[rollback_id]))

                # Rollback states
                self._seq_num = rollback_id + 1
                self._state = self._checkpoints[rollback_id]

                # Remove all snapshots later than the rollback point
                pop_ids = [k for k in self._checkpoints if k > self._seq_num]
                for id in pop_ids:
                    self._checkpoints.pop(id)
                self._logger.info("Rollback to SNAPSHOT ID %d" % rollback_id)

    def rollback_to_beginning(self):
        # Assume sink didn't process any messages, so start over
        self._seq_num = None
        self._state = deque()
        self._checkpoints = dict()
        self.reset_progress(Timestamp(coordinates=[0]))
        self._logger.info("Rollback to START OVER")

    def execute(self):
        self.spin()