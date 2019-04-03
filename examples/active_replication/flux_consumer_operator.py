import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging


class FluxConsumerOperator(Op):
    def __init__(self,
                 name,
                 primary,
                 output_stream_name,
                 ack_stream_name,
                 log_file_name=None):
        super(FluxConsumerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._primary = primary
        self._output_stream_name = output_stream_name
        self._ack_stream_name = ack_stream_name
        
    @staticmethod
    def setup_streams(input_streams, output_stream_name, ack_stream_name):

        input_streams.add_callback(FluxConsumerOperator.on_msg)

        return [DataStream(name=output_stream_name),
                DataStream(name=ack_stream_name,
                           labels={'ack_stream': 'true'})]

    def on_msg(self, msg):
        # 1) Extract the sequence number out of the message
        (msg_seq_num, data) = msg.data
        msg.data = data
        # 1) ACK the message
        self._get_output_stream(self._ack_stream_name).send(
            Message(msg_seq_num, msg.timestamp))
        # 2) Forward the message
        self.get_output_stream(self._output_stream_name).send(msg)

    def execute(self):
        self.spin()
