from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging
import flux_utils


class FluxConsumerOperator(Op):
    def __init__(self,
                 name,
                 replica_num,
                 output_stream_name,
                 ack_stream_name,
                 log_file_name=None):
        super(FluxConsumerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._replica_num = replica_num
        self._output_stream_name = output_stream_name
        self._ack_stream_name = ack_stream_name
        self._failed = False
        
    @staticmethod
    def setup_streams(input_streams, output_stream_name, ack_stream_name):

        input_streams.add_callback(FluxConsumerOperator.on_msg)

        return [DataStream(name=output_stream_name,
                           labels={'back_pressure': 'true'}),
                DataStream(name=ack_stream_name,
                           labels={'ack_stream': 'true'})]

    def on_msg(self, msg):
        if not self._failed:
            # Remove ingress seq num
            # print('%s received %s' % (self.name, msg))
            (msg_seq_num, data) = msg.data
            msg.data = data
            # 1) ACK the message
            self.get_output_stream(self._ack_stream_name).send(
                Message((self._replica_num, msg_seq_num), msg.timestamp))
            # 2) Forward the message
            self.get_output_stream(self._output_stream_name).send(msg)

    def on_control_msg(self, msg):
        control_num = int(msg.data)
        if self._replica_num == control_num:   # Fail
            self._failed = True
        elif self._failed and control_num == flux_utils.FluxControllerCommand.RECOVER:
            self._failed = False

    def execute(self):
        self.spin()
