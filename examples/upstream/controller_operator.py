import time
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging
import upstream_util


class ControllerOperator(Op):
    def __init__(self,
                 name,
                 pre_failure_time_elapse_s=None,
                 post_failure_time_elapse_s=None,
                 log_file_name=None):
        super(ControllerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._pre_failure_time_elapse_s = pre_failure_time_elapse_s  # seconds elapsed before failure trigger
        self._post_failure_time_elapse_s = post_failure_time_elapse_s  # seconds elapsed before failure trigger

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_completion_callback(ControllerOperator.on_watermark_msg)
        return [DataStream(name='failure_stream',
                           labels={'failure': 'true', 'no_watermark': 'true'}),
                DataStream(name='progress_stream',
                           labels={'progress': 'true', 'no_watermark': 'true'})]

    def on_watermark_msg(self, msg):
        # Controller receives snapshot ID from sink every sink takes one
        progress = msg.timestamp.coordinates[0]
        self._logger.info('received progress %d' % progress)
        msg = Message((upstream_util.UpstreamControllerCommand.PROGRESS, progress),
                      Timestamp(coordinates=[0]))
        self.get_output_stream('progress_stream').send(msg)

    def execute(self):
        if self._pre_failure_time_elapse_s is not None:
            time.sleep(self._pre_failure_time_elapse_s)
            failure_msg = Message(upstream_util.UpstreamControllerCommand.FAIL,
                               Timestamp(coordinates=[0]))
            pub = self.get_output_stream('failure_stream')
            pub.send(failure_msg)
            self._logger.info("Controller send failure message.")

            if self._post_failure_time_elapse_s is not None:
                time.sleep(self._post_failure_time_elapse_s)
                recover_msg = Message(upstream_util.UpstreamControllerCommand.RECOVER,
                                      Timestamp(coordinates=[0]))
                pub.send(recover_msg)
                self._logger.info("Controller send recovery message.")
        self.spin()