from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_control_stream, is_not_control_stream
from flux_buffer import Buffer


class FluxEgressOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 ack_stream_name,
                 num_replicas=2,
                 log_file_name=None):
        super(FluxEgressOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._output_stream_name = output_stream_name
        self._ack_stream_name = ack_stream_name
        self._num_replicas = num_replicas
        self._buffer = Buffer(num_replicas)

    @staticmethod
    def setup_streams(input_streams,
                      output_stream_name,
                      ack_stream_name):

        input_streams.filter(is_not_control_stream).add_callback(
            FluxEgressOperator.on_msg)

        input_streams.filter(is_control_stream).add_callback(
            FluxEgressOperator.on_control_msg)

        return [DataStream(name=output_stream_name),
                DataStream(name=ack_stream_name,
                           labels={'ack_stream': 'true'})]

    def on_msg(self, msg):
        msg_seq_num = msg.data[0]
        # Send ACK message to replica if we have one.
        if self._num_replicas > 1:
            self.get_output_stream(self._ack_stream_name).send(
                Message(msg_seq_num, msg.timestamp))
        msg.data = msg.data[1]  # Remove the output sequence number
        # Forward output
        self.get_output_stream(self._output_stream_name).send(msg)
        # TODO(yika): optionally buffer data until sink sends ACK

    def on_control_msg(self, msg):
        if msg.data == 0:
            self._num_replicas -= 1
        elif msg.data > 0:
            self._num_replicas -= 1
        if -1 < msg.data < self._num_replicas:
            self._num_replicas -= 1
            # Send REVERSE msg to secondary
            msg.data = flux_utils.SpecialCommand.REVERSE
            self.get_output_stream(self._output_stream_name).send(msg)
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))
        
    def execute(self):
        self.spin()
