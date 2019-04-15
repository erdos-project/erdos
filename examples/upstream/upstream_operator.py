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
        self._buffer = deque()
        self._downstream_failed = False

    @staticmethod
    def setup_streams(input_streams):
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
        self._buffer.append(int(msg.data))
        # Send msg
        if not self._downstream_failed:
            self.get_output_stream("failure_op_out").send(msg)

    def on_failure_msg(self, msg):
        if msg.data == upstream_util.UpstreamControllerCommand.FAIL:
            self._downstream_failed = True
        elif msg.data == upstream_util.UpstreamControllerCommand.RECOVER:
            self._downstream_failed = False
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))

    def on_progress_msg(self, msg):
        (control_msg, progress) = msg.data
        if control_msg == upstream_util.UpstreamControllerCommand.PROGRESS:
            while len(self._buffer) > 0 and self._buffer[0] <= progress:
                self._buffer.popleft()

    def execute(self):
        self.spin()