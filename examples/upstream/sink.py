from erdos.op import Op
from erdos.utils import setup_logging


class Sink(Op):
    def __init__(self, name, log_file_name=None):
        super(Sink, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self.last_received_num = None

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(Sink.on_msg)
        return []

    def on_msg(self, msg):
        seq_num = int(msg.data)
        # Check duplicate
        if self.last_received_num is None:
            self.last_received_num = seq_num
        elif self.last_received_num + 1 == seq_num:
            self._logger.info('received %d' % seq_num)
            self.last_received_num = seq_num
        else:  # sink receives duplicates
            self._logger.info('received DUPLICATE or WRONG-ORDER message %d' % seq_num)

    def execute(self):
        self.spin()