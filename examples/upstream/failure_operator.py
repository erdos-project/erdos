from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
import upstream_util
from upstream_util import is_control_stream, is_not_control_stream


class FailureOperator(Op):
    def __init__(self,
                 name,
                 log_file_name=None):
        super(FailureOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._failed = False

    @staticmethod
    def setup_streams(input_streams):
        input_streams\
            .filter(is_not_control_stream)\
            .add_callback(FailureOperator.on_msg)
        input_streams\
            .filter(is_control_stream)\
            .add_callback(FailureOperator.on_fail_msg)
        return [DataStream(name="failure_out")]

    def on_msg(self, msg):
        if not self._failed:
            # Send msg
            self.get_output_stream("failure_out").send(msg)

    def on_fail_msg(self, msg):
        if msg.data == upstream_util.UpstreamControllerCommand.FAIL:
            self._failed = True
            self._logger.info("Failure op failed by controller.")
        elif self._failed and msg.data == upstream_util.UpstreamControllerCommand.RECOVER:
            self._failed = False
            self._logger.info("Failed op recovered by controller.")
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))

    def execute(self):
        self.spin()