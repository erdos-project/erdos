from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_control_stream, is_not_control_stream


class FailureOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 primary,
                 log_file_name=None):
        super(FailureOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._output_stream_name = output_stream_name
        self._primary = primary
        self._faied = False
        self._num_msgs = 0
        
    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams.filter(is_not_control_stream).add_callback(
            FailureOperator.on_msg)
        input_streams.filter(is_control_stream).add_callback(
            FailureOperator.on_failure_msg)
        return [DataStream(name=output_stream_name)]

    def on_msg(self, msg):
        if not self._failed:
            self._num_msgs += 1
            self.get_output_stream(self._output_stream_name).send(msg)

    def on_failure_msg(self, msg):
        if msg.data == flux_utils.FAILED_REPLICA and not self._primary:
            self._failed = True
        elif msg.data == flux_utils.FAILED_PRIMARY and self._primary:
            self._failed = True

    def execute(self):
        self.spin()
