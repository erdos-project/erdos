from collections import deque
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_control_stream, is_not_control_stream


class FluxEgressOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 ack_stream_name,
                 log_file_name=None):
        super(FluxEgressOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._output_stream_name = output_stream_name
        self._ack_stream_name = ack_stream_name
        self._ack_required = True
        self._buffer = deque()

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
        # Send ACK message if we have a replica.
        if self._ack_required is True:
            # Send ACK to the secondary Flux producer.
            self.get_output_stream(self._ack_stream_name).send(
                Message(msg_seq_num, msg.timestamp))
        # Remove the output sequence number from the message.
        msg.data = msg.data[1]
        # Forward output.
        self.get_output_stream(self._output_stream_name).send(msg)
        # XXX(ionel): We should ask the downstream to send back an ACK
        # message. We would buffer the message until we receive back an ACK.

    def on_control_msg(self, msg):
        if msg.data == flux_utils.FAILED_REPLICA:
            # Not required to send an ACK anymore because there's no replica.
            self._ack_required = False
        elif msg.data == flux_utils.FAILED_PRIMARY:
            self._ack_required = False
            # TODO(ionel): We should back a failure notification to the
            # secondary so that it can take primary role. We currently send
            # that notification directly via the control channel, but that
            # may cause some messages to be delivered twice or missed. Instead
            # if we were to submit it from here, we would be sure no messages
            # and ACKs are lost because streams are reliable and deliver
            # messages in order.
            pass
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))
        
    def execute(self):
        self.spin()
