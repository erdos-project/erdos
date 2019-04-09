from collections import deque
from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging

import flux_utils
from flux_utils import is_ack_stream, is_not_ack_stream, is_not_back_pressure
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
        # ex_secondary indicates if this primary producer was previously a secondary,
        # used for exactly-once-delivery at the beginning of the take-over
        self._ex_secondary = False
        self._output_stream_name = output_stream_name
        self._output_seq_num = 0
        if replica_num > 0:
            # only replicas have these buffers to keep track
            # with the progress of primary op
            self._buffer = Buffer(1)
            self._ack_buffer = deque()
        self._lock = threading.Lock()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams\
            .filter(is_not_ack_stream)\
            .filter(is_not_back_pressure)\
            .add_callback(FluxProducerOperator.on_msg)
        input_streams.filter(is_ack_stream).add_callback(
            FluxProducerOperator.on_ack_msg)

        return [DataStream(name=output_stream_name)]

    def on_msg(self, msg):
        self._lock.acquire()
        # XXX(ionel): Need to ensure that we remove output_seq_num
        # during recovery.
        if self._replica_num == 0:  # Primary
            msg.data = (self._output_seq_num, msg.data)
            if self._ex_secondary:  # used to a secondary
                self._catch_up_progress(msg)
            else:
                self.get_output_stream(self._output_stream_name).send(msg)
        else:
            if self._output_seq_num == self._ack_buffer[0]:    # ACK already received
                self._ack_buffer.popleft()
            else:
                self._buffer.put(msg.data, self._output_seq_num, self._replica_num)
        self._output_seq_num += 1
        self._lock.release()

    def on_ack_msg(self, msg):
        self._lock.acquire()
        if msg.data == flux_utils.SpecialCommand.REVERSE:
            # Secondary receives a REVERSE message from egress
            # turn into a primary producer
            self._replica_num = 0
            self._ex_secondary = True
            self._logger.info("Secondary producer took over.")
            # Need to ensure that no messages are lost or duplicated.
            # Check if the primary was running before the secondary (already
            # received ACKs for messages that the secondary hasn't produced
            # yet)
            if len(self._ack_buffer) > 0:
                assert self._buffer.size() == 0
            else:
                # Secondary was ahead of the primary => send out buffered messages.
                pub = self.get_output_stream(self._output_stream_name)
                data = self._buffer.pop_oldest()[1]
                while data is not None:
                    pub.send(data)
                    data = self._buffer.pop_oldest()[1]
        else:   # Secondary receives ACK messages
            msg_seq_num = int(msg.data)
            if self._buffer.size() > 0: # ahead
                assert self._buffer.match_oldest(msg_seq_num) is True
                self._buffer.pop_oldest()
            else:   # behind
                self._ack_buffer.append(msg_seq_num)
        self._lock.release()

    def _catch_up_progress(self, msg):
        # used by an ex-secondary primary producer who just took over
        # and need to catch up with the progress of the failed primary
        if len(self._ack_buffer) > 0 and self._ack_buffer[-1] > self._output_seq_num:  # behind progress
            assert self._output_seq_num == self._ack_buffer[0]
            self._ack_buffer.popleft()
        else:  # ahead of progress
            self.get_output_stream(self._output_stream_name).send(msg)

    def execute(self):
        self.spin()
