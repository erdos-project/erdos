import time
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging


class Source(Op):
    def __init__(self,
                 name,
                 num_messages=50,
                 fps=10,
                 log_file_name=None):
        super(Source, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._cnt = 0
        self._seq_num = 0
        self._num_messages = num_messages
        self._time_gap = 1.0 / fps

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(name='input_stream')]

    def execute(self):
        while self._seq_num < self._num_messages:
            output_msg = Message("data-"+str(self._seq_num),
                                 Timestamp(coordinates=[self._seq_num]))
            self.get_output_stream('input_stream').send(output_msg)
            self._seq_num += 1
            time.sleep(self._time_gap)
