from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_ack_stream, is_control_stream, is_not_ack_stream, is_not_control_stream
from flux_buffer import Buffer
import threading


class FluxProducerOperator(Op):
    def __init__(self,
                 name,
                 replica_num,
                 output_stream_name,
                 log_file_name=None):
        super(FluxProducerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._replica_num = replica_num
        self._ex_secondary = False
        self._output_stream_name = output_stream_name
        self._output_seq_num = 0
        if replica_num > 0:
            # only replicas have these buffers to keep track
            # with the progress of primary op
            self._buffer = Buffer(1)
            self._ack_buffer = set()
        # atomic
        self.input_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.control_lock = threading.Lock()

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
        # XXX(ionel): Need to ensure that we remove output_seq_num
        # during recovery.
        self.input_lock.acquire()
        if self._replica_num == 0:  # Primary
            msg.data = (self._output_seq_num, msg.data)
            if self._ex_secondary:  # used to a secondary
                if len(self._ack_buffer) > 0:   # behind progress
                    assert self._output_seq_num in self._ack_buffer
                    self._ack_buffer.remove(self._output_seq_num)
                else:   # ahead of progress
                    self.get_output_stream(self._output_stream_name).send(msg)
            else:
                self.get_output_stream(self._output_stream_name).send(msg)
        else:
            if self._output_seq_num in self._ack_buffer:    # ACK already received
                self._ack_buffer.remove(self._output_seq_num)
            else:
                self._buffer.put(msg.data, self._output_seq_num)
        self._output_seq_num += 1
        self.input_lock.release()

    def on_ack_msg(self, msg):
        self.ack_lock.acquire()
        if msg.data == flux_utils.SpecialCommand.REVERSE:
            # Secondary receives a REVERSE message from egress
            # turn into a primary producer
            self._replica_num = 0
            self._ex_secondary = True
            # Need to ensure that no messages are lost or duplicated.
            # Check if the primary was running before the secondary (already
            # received ACKs for messages that the secondary hasn't produced
            # yet)
            if len(self._ack_buffer) > 0:
                assert self._buffer.size() == 0
            else:
                # Secondary was ahead of the primary => send out buffered messages.
                self._buffer.send_and_clear(self.get_output_stream(self._output_stream_name))
        else:   # Secondary receives ACK messages
            assert self._replica_num > 0
            msg_seq_num = msg.data
            if self._buffer.size() > 0: # ahead
                assert self._buffer.match_oldest(msg_seq_num) is True
                self._buffer.pop_oldest()
            else:   # behind
                self._ack_buffer.add(msg_seq_num)
        self.ack_lock.release()

    def execute(self):
        self.spin()
