from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging


class GenericLoadOperator(Op):
    def __init__(self, name):
        super(GenericLoadOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')

    @staticmethod
    def setup_streams(input_streams, output_types):
        output_streams = []
        for (name, data_type) in output_types:
            output_streams.append(DataStream(data_type=data_type, name=name))
        return output_streams

    def execute(self):
        self.spin()
