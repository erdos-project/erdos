from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
from collections import deque
import upstream_util


class UpstreamOperator(Op):
    def __init__(self,
                 name,
                 log_file_name=None):
        super(UpstreamOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._msg_buffer = deque()
        self._watermark = None
        self._downstream_failed = False

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_completion_callback(UpstreamOperator.on_watermark_msg)
        input_streams\
            .filter(upstream_util.is_progress_stream)\
            .add_callback(UpstreamOperator.on_progress_msg)
        input_streams\
            .filter(upstream_util.is_failure_stream)\
            .add_callback(UpstreamOperator.on_failure_msg)
        input_streams\
            .filter(upstream_util.is_not_progress_stream)\
            .filter(upstream_util.is_not_failure_stream)\
            .add_callback(UpstreamOperator.on_msg)
        return [DataStream(name="failure_op_out")]

    def on_msg(self, msg):
        # Buffer msg
        self._msg_buffer.append(msg)
        # Send msg
        if not self._downstream_failed:
            self.get_output_stream("failure_op_out").send(msg)
    
    def on_watermark_msg(self, msg):
        if self._downstream_failed:
            self._watermark = msg  # make sure this is the latest progress
        else:
            self.get_output_stream("failure_op_out").send(msg)
        
    def on_failure_msg(self, msg):
        if msg.data == upstream_util.UpstreamControllerCommand.FAIL:
            self._downstream_failed = True
        elif msg.data == upstream_util.UpstreamControllerCommand.RECOVER:
            # Upon recovery, send all data in msg buffer and the latest watermark
            pub = self.get_output_stream("failure_op_out")
            while len(self._msg_buffer) > 0:
                pub.send(self._msg_buffer.popleft())
            if self._watermark is not None:
                pub.send(self._watermark)
            self._downstream_failed = False
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))

    def on_progress_msg(self, msg):
        (control_msg, progress) = msg.data
        if control_msg == upstream_util.UpstreamControllerCommand.PROGRESS:
            while len(self._msg_buffer) > 0 and int(self._msg_buffer[0].data) <= progress:
                self._msg_buffer.popleft()

    def execute(self):
        self.spin()