from erdos.op import Op
from erdos.utils import setup_logging
from flux_utils import is_not_ack_stream


class Sink(Op):
    def __init__(self, name, log_file_name=None):
        super(Sink, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self.last_received_num = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(is_not_ack_stream).add_callback(Sink.on_msg)
        return []

    def on_msg(self, msg):
        print('%s received %s' % (self.name, msg))
        num = int(msg.data.split("-")[1])
        if self.last_received_num is not None:
            assert num == self.last_received_num + 1, \
                "Sink received wrong ordered or duplicated message: last (%d) vs current (%d)" \
                % (self.last_received_num, num)
        self.last_received_num = num

    def execute(self):
        self.spin()