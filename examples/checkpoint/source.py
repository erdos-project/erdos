import time
from erdos.data_stream import DataStream
from erdos.message import Message, WatermarkMessage
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import setup_logging
import checkpoint_util


class Source(Op):
    def __init__(self,
                 name,
                 checkpoint_enable=True,
                 checkpoint_freq=10,
                 num_messages=50,
                 fps=10,
                 log_file_name=None):
        super(Source, self).__init__(name,
                                     checkpoint_enable=checkpoint_enable,
                                     checkpoint_freq=checkpoint_freq)
        self._logger = setup_logging(self.name, log_file_name)
        self._seq_num = 1
        self._num_messages = num_messages
        self._time_gap = 1.0 / fps

    @staticmethod
    def setup_streams(input_streams):
        input_streams\
            .filter(checkpoint_util.is_control_stream)\
            .add_callback(Source.on_rollback_msg)
        return [DataStream(name='input_stream')]

    def on_rollback_msg(self, msg):
        # XXX(Yika): fake rollback since source op does not really checkpoint
        if msg.data == checkpoint_util.CheckpointControllerCommand.ROLLBACK:
            self._seq_num = msg.timestamp.coordinates[0] + 1

    def execute(self):
        while self._seq_num < self._num_messages:
            # Send msg and watermark
            timestamp = Timestamp(coordinates=[self._seq_num])
            output_msg = Message(self._seq_num, timestamp)
            watermark = WatermarkMessage(timestamp)
            pub = self.get_output_stream('input_stream')
            pub.send(output_msg)
            pub.send(watermark)
            self._seq_num += 1
            time.sleep(self._time_gap)
