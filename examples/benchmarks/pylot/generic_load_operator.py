from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.utils import setup_logging


class GenericLoadOperator(LoggingOp):
    def __init__(self, name, buffer_logs=False):
        super(GenericLoadOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')

    @staticmethod
    def setup_streams(input_streams, output_types):
        output_streams = []
        for (name, data_type) in output_types:
            output_streams.append(DataStream(data_type=data_type, name=name))
        return output_streams

    def execute(self):
        self.spin()
