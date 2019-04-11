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
        self._checkpoint_freq = checkpoint_freq
        self._checkpoints = dict()
        self._seq_num = None
        self.last_received_num = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(checkpoint_util.is_control_stream) \
            .add_callback(Sink.on_rollback_msg)
        input_streams.filter(checkpoint_util.is_not_control_stream) \
            .add_callback(Sink.on_msg)
        return [DataStream(name="sink_snapshot", labels={'no_watermark': 'true'})]

    def on_msg(self, msg):
        self._seq_num = int(msg.data)
        if msg.stream_name == 'watermark':
            self.on_watermark()
        else:
            # Check duplicate
            if self.last_received_num is None:
                self.last_received_num = self._seq_num
            elif self.last_received_num + 1 == self._seq_num:
                self._logger.info('received %d' % self._seq_num)
                self.last_received_num = self._seq_num
                # Build state
                if len(self._state) == self._state_size:  # state is full
                    self._state.popleft()
                self._state.append(self._seq_num)
            else:   # sink receives duplicates
                self._logger.info('received DUPLICATE %d' % self._seq_num)

    def checkpoint(self):
        # Override base class checkpoint function
        snapshot_id = self._state[-1]  # latest received seq num/timestamp
        assert snapshot_id not in self._checkpoints
        self._checkpoints[snapshot_id] = copy(self._state)
        self._logger.info('checkpointed at latest stored data %d' % snapshot_id)

        # Send snapshot ID (latest received seq num) to controller
        snapshot_msg = Message(snapshot_id, timestamp=None)
        self.get_output_stream("sink_snapshot").send(snapshot_msg)

    def on_rollback_msg(self, msg):
        (control_msg, rollback_id) = msg.data
        if control_msg == checkpoint_util.CheckpointControllerCommand.ROLLBACK:
            if rollback_id is None:
                # Assume sink didn't process any messages, so start over
                self._seq_num = None
                self._state = deque()
                self._checkpoints = dict()
                self.reset_progress(Timestamp(coordinates=[0]))
                self._logger.info("Rollback to START OVER")
            else:
                rollback_id = int(rollback_id)
                self._seq_num = rollback_id + 1
                self._state = self._checkpoints[rollback_id]
                # Remove all snapshots later than the rollback point
                for k in self._checkpoints:
                    if k > self._seq_num:
                        self._checkpoints.pop(k)
                self.reset_progress(Timestamp(coordinates=[rollback_id]))
                self._logger.info("Rollback to SNAPSHOT ID %d" % rollback_id)

    def execute(self):
        self.spin()