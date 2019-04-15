from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging


class FailureOperator(Op):
    def __init__(self,
                 name,
                 log_file_name=None):
        super(FailureOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._failed = False

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(FailureOperator.on_msg)
        return [DataStream(name="failure_out")]

    def on_msg(self, msg):
        self.get_output_stream("failure_out").send(msg)

    def execute(self):
        self.spin()