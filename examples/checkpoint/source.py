import time
from copy import copy
from collections import deque
from erdos.data_stream import DataStream
from erdos.message import Message, WatermarkMessage
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging
import checkpoint_util


class Source(Op):
    def __init__(self,
                 name,
                 checkpoint_enable=True,
                 checkpoint_freq=10,
                 state_size=10,
                 num_messages=50,
                 fps=10,
                 log_file_name=None):
        super(Source, self).__init__(name,
                                     checkpoint_enable=checkpoint_enable,
                                     checkpoint_freq=checkpoint_freq)
        self._logger = setup_logging(self.name, log_file_name)
        self._seq_num = 1
        self._num_messages = num_messages
        self._time_gap = 1.0 / fps

        self._state_size = state_size
        self._state = deque()
        self._checkpoints = dict()


    @staticmethod
    def setup_streams(input_streams):
        input_streams\
            .filter(checkpoint_util.is_control_stream)\
            .add_callback(Source.on_rollback_msg)
        return [DataStream(name='input_stream')]

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
                self._reset_progress(Timestamp(coordinates=[rollback_id]))

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
        self._seq_num = 1
        self._state = deque()
        self._checkpoints = dict()
        self._reset_progress(Timestamp(coordinates=[0]))
        self._logger.info("Rollback to START OVER")

    def checkpoint_condition(self, timestamp):
        if timestamp.coordinates[0] % self._checkpoint_freq == 0:
            return True
        return False

    def checkpoint(self):
        snapshot_id = self._state[-1]  # latest received seq num/timestamp
        assert snapshot_id not in self._checkpoints
        self._checkpoints[snapshot_id] = copy(self._state)
        self._logger.info('checkpointed at latest stored data %d' % snapshot_id)

    def execute(self):
        while self._seq_num < self._num_messages:
            # Build state
            if len(self._state) == self._state_size:  # state is full
                self._state.popleft()
            self._state.append(self._seq_num)

            # Send msg and watermark
            output_msg = Message(self._seq_num,
                                 Timestamp(coordinates=[self._seq_num]))
            watermark = WatermarkMessage(Timestamp(coordinates=[self._seq_num]))
            pub = self.get_output_stream('input_stream')
            pub.send(output_msg)
            pub.send(watermark)

            # Checkpoint, source needs to write its own checkpoint
            if self._seq_num % self._checkpoint_freq == 0:
                self.checkpoint()

            self._seq_num += 1
            time.sleep(self._time_gap)
