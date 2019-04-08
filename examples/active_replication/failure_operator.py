from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_control_stream, is_flux_consumer_output


class FailureOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 replica_num,
                 log_file_name=None):
        super(FailureOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._output_stream_name = output_stream_name
        self._replica_num = replica_num
        self._failed = False
        
    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams.filter(is_flux_consumer_output).add_callback(
            FailureOperator.on_msg)
        input_streams.filter(is_control_stream).add_callback(
            FailureOperator.on_control_msg)
        return [DataStream(name=output_stream_name)]

    def on_msg(self, msg):
        if not self._failed:
            self.get_output_stream(self._output_stream_name).send(msg)

    def on_control_msg(self, msg):
        (control_num, replica_num) = msg.data
        if replica_num != self._replica_num:
            pass
        elif control_num == flux_utils.FluxControllerCommand.FAIL:
            self._logger.info("Failed by controller.")
            self._failed = True
        elif self._failed and control_num == flux_utils.FluxControllerCommand.RECOVER:
            self._failed = False
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))

    def execute(self):
        self.spin()
