from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging
from flux_utils import is_ack_stream, is_control_stream, is_not_ack_stream, is_not_control_stream, is_not_back_pressure
from flux_buffer import Buffer
import flux_utils
import threading


class FluxIngressOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 num_replics=2,
                 log_file_name=None):
        super(FluxIngressOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._num_replicas = num_replics
        self._output_stream = output_stream_name
        self._input_msg_seq_num = 0
        self._buffer = Buffer(num_replics)   # buffer to store unacknowledged tuples
        self._status = {}
        for i in range(self._num_replicas):
            self._status[i] = flux_utils.FluxOperatorState.ACTIVE
        # lock not necessary in current ROS/Ray executor,
        # implemented in case we'll change into multi threaded spinner executor
        self._lock = threading.Lock()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams.filter(is_not_ack_stream)\
            .filter(is_not_control_stream)\
            .filter(is_not_back_pressure)\
            .add_callback(FluxIngressOperator.on_msg)     # Input
        input_streams.filter(is_ack_stream).add_callback(
            FluxIngressOperator.on_ack_msg)     # Ack
        input_streams.filter(is_control_stream).add_callback(
            FluxIngressOperator.on_control_msg)     # Control

        return [DataStream(name=output_stream_name)]

    def on_msg(self, msg):
        self._lock.acquire()
        # Put msg in buffer
        for i in range(self._num_replicas):
            if self._status[i] == flux_utils.FluxOperatorState.ACTIVE:
                self._buffer.put(msg.data, self._input_msg_seq_num, i)
        # Send message to the two downstream Flux Consumer Operators
        msg.data = (self._input_msg_seq_num, msg.data)
        self.get_output_stream(self._output_stream).send(msg)
        # Each input message is assigned a monotonically increasing sequence number
        self._input_msg_seq_num += 1
        self._lock.release()

    def on_ack_msg(self, msg):
        # TODO(yika): optionally send ack to source after dropping
        self._lock.acquire()
        (dest, msg_seq_num) = msg.data
        ack = self._buffer.ack(int(msg_seq_num), int(dest))
        if not ack:
            self._logger.fatal('Received ACK on unexpected stream {}; dest: {}, seq:{}'
                               .format(msg.stream_name, str(dest), str(msg_seq_num)))
        self._lock.release()

    # invoked by controller
    def on_control_msg(self, msg):
        self._lock.acquire()
        (control_num, replica_num) = msg.data
        if control_num == flux_utils.FluxControllerCommand.FAIL:
            self._status[replica_num] = flux_utils.FluxOperatorState.DEAD
            self._buffer.ack_all(replica_num)
        elif control_num == flux_utils.FluxControllerCommand.RECOVER:
            self._status[replica_num] = flux_utils.FluxOperatorState.ACTIVE
        else:
            self._logger.fatal('Unexpected control message {}'.format(msg))
        self._lock.release()

    def execute(self):
        self.spin()
