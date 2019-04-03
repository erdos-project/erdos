from collections import deque
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_ack_stream, is_control_stream, is_not_ack_stream, is_not_control_stream


class FluxIngressOperator(Op):
    def __init__(self,
                 name,
                 primary_stream_name,
                 secondary_stream_name,
                 primary_ack_stream_name,
                 secondary_ack_stream_name,
                 log_file_name=None):
        super(FluxIngressOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._num_replicas = 2
        self._output_streams = [primary_stream_name, secondary_stream_name]
        self._primary_ack_stream_name = primary_ack_stream_name
        self._secondary_ack_stream_name = secondary_ack_stream_name
        # The size of the buffer limits the drift of the two copies.
        # In this implementation, there's no upper bound on drift.
        self._primary_buffer = deque()
        self._secondary_buffer = deque()

    @staticmethod
    def setup_streams(input_streams,
                      primary_stream_name,
                      secondary_stream_name):
        input_streams.filter(is_not_ack_stream).filter(is_not_control_stream).add_callback(
            FluxIngressOperator.on_msg)
        input_streams.filter(is_ack_stream).add_callback(
            FluxIngressOperator.on_ack_msg)
        input_streams.filter(is_control_stream).add_callback(
            FluxIngressOperator.on_control_msg)

        return [DataStream(name=primary_stream_name),
                DataStream(name=secondary_stream_name)]

    def on_msg(self, msg):
        # Each input message is assigned a monotonically increasing
        # sequency number.
        self._input_msg_seq_num += 1
        # Buffer message until is ACKed. We store the message together with the
        # number of ACKs we expect to receive.
        buf_msg = (msg, self._num_replicas)
        self._primary_buffer.append(buf_msg)
        self._primary_buffer.append(buf_msg)
        # Send message to the two downstream Flux Consumer Operators.
        msg.data = (self._input_msg_seq_num, msg.data)
        for stream_name in self._output_streams:
            self.get_output_stream(stream_name).send(msg)

    def on_ack_msg(self, msg):
        msg_seq_num = msg.data
        if msg.stream_name == self._primary_ack_stream_name:
            # Remove the message from the primary buffer
            (seq_num, buf_msg) = self._primary_buffer.popleft()
            assert msg_seq_num == seq_num
        elif msg.stream_name == self._secondary_ack_stream_name:
            # Remove the message from the secondary buffer
            (seq_num, buf_msg) = self._secondary_buffer.popleft()
            assert msg_seq_num == seq_num
        else:
            self._logger.fatal('Received ACK on unexpected stream {}'.format(
                msg.stream_name))
        # TODO(ionel): The ingress operator should send an ACK to the
        # source once it has incorporated the input into the data-flow.
        # In other words, after it has removed the message from both buffers.
        
    def on_control_msg(self, msg):
        assert len(self._output_streams) == 2
        if msg.data == flux_utils.FAILED_REPLICA:
            self._num_replicas -= 1
            # We no longer need to account for the secondary replica.
            self._secondary_buffer.clear()
            self._output_streams = self._output_streams[:1]
        elif msg.data == flux_utils.FAILED_PRIMARY:
            self._num_replicas -= 1
            # We no longer need to account for the primary replica.
            self._primary_buffer.clear()
            self._output_streams = self._output_streams[1:]
        else:
            self._logger.fatal('Unexpected control messsage {}'.format(msg))

        assert self._num_replicas > 0

    def execute(self):
        self.spin()
