from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
from collections import deque
import upstream_util
from upstream_util import is_not_control_stream, is_control_stream


class UpstreamOperator(Op):
    def __init__(self,
                 name,
                 log_file_name=None):
        super(UpstreamOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._buffer = deque()

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(is_control_stream)\
            .add_callback(UpstreamOperator.on_progress_msg)
        input_streams.filter(is_not_control_stream)\
            .add_callback(UpstreamOperator.on_msg)
        return [DataStream(name="failure_op_out")]

    def on_msg(self, msg):
        # Buffer msg
        self._buffer.append(int(msg.data))
        # Send msg
        self.get_output_stream("failure_op_out").send(msg)

    def on_progress_msg(self, msg):
        (control_msg, progress) = msg.data
        if control_msg == upstream_util.CheckpointControllerCommand.PROGRESS:
            assert progress == self._buffer[0]  # because sink forwards 1 watermark after every msg received
            self._buffer.popleft()

    def execute(self):
        self.spin()