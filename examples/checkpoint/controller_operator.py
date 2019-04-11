import time

from erdos.data_stream import DataStream
from erdos.message import Message, WatermarkMessage
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging
import checkpoint_util
import sys


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
        input_streams.add_callback(ControllerOperator.on_snapshot_msg)
        return [DataStream(name='controller_stream',
                           labels={'control_stream': 'true',
                                   'no_watermark': 'true'})]

    def on_snapshot_msg(self, msg):
        # Controller receives snapshot ID from sink every sink takes one
        self._sink_snapshot_id = int(msg.data)
        self._logger.info('received sink SNAPSHOT ID %d' % self._sink_snapshot_id)

    def execute(self):
        if self._pre_failure_time_elapse_s is not None:
            time.sleep(self._pre_failure_time_elapse_s)
            rollback_msg = Message((checkpoint_util.CheckpointControllerCommand.ROLLBACK, self._sink_snapshot_id),
                               Timestamp(coordinates=[0]))
            pub = self.get_output_stream('controller_stream')
            pub.send(rollback_msg)
            self._logger.info("Control send rollback message to everyone")

        self.spin()
