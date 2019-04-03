from collections import deque
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_ack_stream, is_control_stream, is_not_ack_stream, is_not_control_stream


class FluxProducerOperator(Op):
    def __init__(self,
                 name,
                 primary,
                 output_stream_name,
                 log_file_name=None):
        super(FluxProducerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._primary = primary
        self._ex_secondary = False
        self._output_stream_name = output_stream_name
        self._output_seq_num = 0
        # Buffer to be used when the replica is running ahead of the primary.
        self._buffer = deque()
        # Buffer to be used when the replica is running behind the primary.
        self._ack_buffer = deque()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):

        input_streams.filter(is_not_control_stream).filter(is_not_ack_stream).add_callback(
                FluxProducerOperator.on_msg)
        input_streams.filter(is_ack_stream).add_callback(
            FluxProducerOperator.on_ack_msg)
        input_streams.filter(is_control_stream).add_callback(
            FluxProducerOperator.on_control_msg)

        return [DataStream(name=output_stream_name)]

    def on_msg(self, msg):
        # Each output message is assigned a monotonically increasing
        # sequence number.
        self._output_seq_num += 1
        # XXX(ionel): Need to ensure that we remove output_seq_num
        # during recovery.
        msg.data = (self._output_seq_num, msg.data)
        if self._primary:
            # Check if it used to be a secondary node.
            if self._ex_secondary:
                if len(self._ack_buffer) > 0:
                    assert self._ack_buffer[0] == self._output_seq_num
                    # Output msg was already send to the egress by the primary.
                    self._ack_buffer.popleft()
                else:
                    # Output msg wasn't already sent. Send it now.
                    self.get_output_stream(self._output_stream_name).send(msg)
            else:
                # Primary didn't use to a secondery. Forward the message to the
                # egress.
                self.get_output_stream(self._output_stream_name).send(msg)
        else:
            if len(self._ack_buffer) == 0:
                # Buffer if output message hasn't already been ACKed
                self._buffer.append(msg)
            else:
                assert self._ack_buffer[0] == self._output_seq_num
                # Delete its corresponding ACK from the buffer
                self._ack_buffer.popleft()

    def on_ack_msg(self, msg):
        # Only secondary receives ACK messages
        assert self._primary is False
        msg_seq_num = msg.data

        if len(self._buffer) > 0:
            assert self._buffer[0].data[0] == msg_seq_num
            # Secondary is up-to-date => we can remove the message from the
            # buffer.
            self._buffer.popleft()
        else:
            # Secondary is behind => buffer ACK so that output message can be
            # discarded when it is produced.
            self._ack_buffer.add(msg_seq_num)

    def on_control_msg(self, msg):
        if msg.data == flux_utils.FAILED_REPLICA:
            assert self._primary
            # The secondary has failed. We can clear up the buffers because
            # we don't need them anymore.
            self._buffer.clear()
            self._ack_buffer.clear()
            # We do not need to do anything else. We can carry on sending data.
        elif msg.data == flux_utils.FAILED_PRIMARY:
            assert not self._primary
            # Primary has failed, the replica in now the primary.
            self._primary = True
            self._ex_secondary = True
            # Need to ensure that no messages are lost or duplicated.

            # Check if the primary was running before the secondary (already
            # received ACKs for messages that the secondary hasn't produced
            # yet)
            if len(self._ack_buffer) > 0:
                assert len(self._buffer) == 0
            else:
                # Secondary was ahead of the primary => send out buffered
                # messages.
                for msg in self._buffer:
                    msg.data = msg.data[1]
                    self.get_output_stream(self._output_stream_name).send(msg)
                self._buffer.clear()
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))

    def execute(self):
        self.spin()
