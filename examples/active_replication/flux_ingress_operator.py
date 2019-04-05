from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
from flux_utils import is_ack_stream, is_control_stream, is_not_ack_stream, is_not_control_stream
from flux_buffer import Buffer
import threading


class FluxIngressOperator(Op):
    def __init__(self,
                 name,
                 output_stream_names,
                 num_replics=2,
                 log_file_name=None):
        super(FluxIngressOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._num_replicas = num_replics
        self._output_streams = output_stream_names
        self._input_msg_seq_num = 0
        self.buffer = Buffer(num_replics)   # buffer to store unacknowledged tuples
        # atomic
        self.input_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.control_lock = threading.Lock()

    @staticmethod
    def setup_streams(input_streams, output_stream_names):
        input_streams.filter(is_not_ack_stream).filter(is_not_control_stream).add_callback(
            FluxIngressOperator.on_msg)     # Input
        input_streams.filter(is_ack_stream).add_callback(
            FluxIngressOperator.on_ack_msg)     # Ack
        input_streams.filter(is_control_stream).add_callback(
            FluxIngressOperator.on_control_msg)     # Control

        output = [DataStream(name=output) for output in output_stream_names]
        return output

    def on_msg(self, msg):
        self.input_lock.acquire()
        # Put msg in buffer
        self.buffer.put(msg, self._input_msg_seq_num)
        # Send message to the two downstream Flux Consumer Operators
        msg.data = (self._input_msg_seq_num, msg.data)
        for stream_name in self._output_streams:
            self.get_output_stream(stream_name).send(msg)
        # Each input message is assigned a monotonically increasing sequence number
        self._input_msg_seq_num += 1
        self.input_lock.release()

    def on_ack_msg(self, msg):
        # TODO(yika): optionally send ack to source after dropping
        self.ack_lock.acquire()
        (dest, msg_seq_num) = msg.data
        ack = self.buffer.ack(msg_seq_num, dest)
        if not ack:
            self._logger.fatal('Received ACK on unexpected stream {}'.format(msg.stream_name))
        self.ack_lock.acquire()

    # invoked by controller
    def on_control_msg(self, msg):
        self.control_lock.acquire()
        failed_replica_num = msg.data
        if -1 < failed_replica_num < self._num_replicas:
            self._num_replicas -= 1
            self._output_streams.pop(failed_replica_num)
            self.buffer.ack_all(failed_replica_num)
        assert self._num_replicas > 0
        self.control_lock.release()

    def execute(self):
        self.spin()
