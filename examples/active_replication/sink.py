from erdos.op import Op
from erdos.utils import setup_logging
from flux_utils import is_not_ack_stream


class Sink(Op):
    def __init__(self, name, log_file_name=None):
        super(Sink, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(is_not_ack_stream).add_callback(Sink.on_msg)
        return []

    def on_msg(self, msg):
        x = '%s received %s' % (self.name, msg)
        print(x)

    def execute(self):
        self.spin()