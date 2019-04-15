import checkpoint_util
from collections import deque
from copy import copy

from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
from erdos.timestamp import Timestamp


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
        # XXX(ionel): _checkpoints could be a queue, but we kept it a dict
        # to be able to easily do asserts.
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

    def checkpoint_condition(self, timestamp):
        if timestamp.coordinates[0] % self._checkpoint_freq == 0:
            return True
        return False

    def checkpoint(self, timestamp):
        # TODO(ionel): Should use timestamp instead of snapshot_id.
        # Override base class checkpoint function
        snapshot_id = self._state[-1]  # latest received seq num/timestamp
        assert snapshot_id not in self._checkpoints
        self._checkpoints[snapshot_id] = copy(self._state)
        self._logger.info('Checkpointed at {}'.format(timestamp))

    def restore(self, timestamp):
        if timestamp is None:
            # Sink did not snapshot anything
            self.rollback_to_beginning()
        else:
            # TODO(ionel): The logic for prunning checkpoints and resetting
            # progress should happen in ERDOS, not in user land.
            timestamp = int(timestamp)
            # Find snapshot id that is the closest to and smaller/equal to the timestamp
            ids = sorted(self._checkpoints.keys())
            timestamp = [id for id in ids if id <= timestamp][-1]

            # Reset watermark
            self._reset_progress(Timestamp(coordinates=[timestamp]))

            # Rollback states
            self._seq_num = timestamp + 1
            self._state = self._checkpoints[timestamp]

            # Remove all snapshots later than the rollback point
            pop_ids = [k for k in self._checkpoints if k > self._seq_num]
            for id in pop_ids:
                self._checkpoints.pop(id)
            self._logger.info("Rollback to SNAPSHOT ID %d" % timestamp)

    def on_rollback_msg(self, msg):
        (control_msg, rollback_id) = msg.data
        if control_msg == checkpoint_util.CheckpointControllerCommand.ROLLBACK:
            # TODO(ionel): Should pass timestamp.
            self.restore(rollback_id)

    def rollback_to_beginning(self):
        # Assume sink didn't process any messages, so start over
        self._seq_num = None
        self._state = deque()
        self._checkpoints = dict()
        self._reset_progress(Timestamp(coordinates=[0]))
        self._logger.info("Rollback to START OVER")

    def execute(self):
        self.spin()
