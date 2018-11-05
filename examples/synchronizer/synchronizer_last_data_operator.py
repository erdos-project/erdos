from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency

from std_msgs.msg import String


class SynchronizerLastDataOperator(Op):
    """Publishes messages at a given frequency. Each time it publishes the
       latest data.
    """

    def __init__(self, name='synchronizer'):
        super(SynchronizerLastDataOperator, self).__init__(name)
        self._last_data = {}
        self._epoch = 0

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(
            SynchronizerLastDataOperator.on_msg_latest_data)
        return [DataStream(data_type=String, name='sync_stream')]

    def on_msg_latest_data(self, msg):
        if (msg.stream_name not in self._last_data
                or msg.timestamp > self._last_data[msg.stream_name].timestamp):
            self._last_data[msg.stream_name] = msg

    @frequency(1)
    def publish_data(self):
        timestamp = Timestamp(coordinates=[self._epoch])
        msg = Message(self._last_data, timestamp)
        print('Sending {}'.format(msg))
        self.get_output_stream('sync_stream').send(msg)
        self._epoch += 1

    def execute(self):
        self.publish_data()
        self.spin()
