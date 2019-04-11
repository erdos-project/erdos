import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging
import checkpoint_util


class ControllerOperator(Op):
    def __init__(self,
                 name,
                 pre_failure_time_elapse_s=None,
                 log_file_name=None):
        super(ControllerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._pre_failure_time_elapse_s = pre_failure_time_elapse_s  # seconds elapsed before failure trigger
        self._sink_snapshot_id = None

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(name='controller_stream',
                           labels={'control_stream': 'true'})]

    def on_snapshot_msg(self, msg):
        # Controller receives snapshot ID from sink every sink takes one
        self._sink_snapshot_id = int(msg.data)

    def execute(self):
        # Send failure message.
        if self._pre_failure_time_elapse_s is not None:
            time.sleep(self._pre_failure_time_elapse_s)
            rollback_msg = Message((checkpoint_util.CheckpointControllerCommand.ROLLBACK, self._sink_snapshot_id),
                               Timestamp(coordinates=[0]))
            self.get_output_stream('controller_stream').send(rollback_msg)
            print("Control send rollback message to everyone")

        self.spin()
