import sys

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp

from std_msgs.msg import String


class SynchronizerDataWithinLimitOperator(Op):
    """Publishes messages when input data is synchronized within a time limit.
    """

    def __init__(self, upper_time_limit, name='synchronizer'):
        super(SynchronizerDataWithinLimitOperator, self).__init__(name)
        self._upper_time_limit = upper_time_limit
        self._stream_data = {}

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(
            SynchronizerDataWithinLimitOperator.on_msg_data_sync_within)
        return [DataStream(data_type=String, name='sync_stream')]

    def on_msg_data_sync_within(self, msg):
        if msg.stream_name in self._stream_data:
            self._stream_data[msg.stream_name].append(msg)
        else:
            self._stream_data[msg.stream_name] = [msg]
        if len(self._stream_data.keys()) == len(self.input_streams):
            empty_msgs = False
            min_stream_name = None
            min_time = sys.maxint
            max_stream_name = None
            max_time = 0
            for stream_name, msgs in self._stream_data.iteritems():
                if len(msgs) == 0:
                    empty_msgs = True
                    break
                if int(msgs[0].data) < min_time:
                    min_time = msgs[0].data
                    min_stream_name = msgs[0].stream_name
                if int(msgs[0].data) > max_time:
                    max_time = msgs[0].data
                    max_stream_name = msgs[0].stream_name
            if empty_msgs:
                # We don't have any messages for some input streams.
                return
            if max_time - min_time <= self._upper_time_limit:
                # Messages are synced within the time limit.
                output = {}
                for stream_name, msgs in self._stream_data.iteritems():
                    output[stream_name] = msgs[0]
                    self._stream_data[stream_name] = msgs[1:]
                output_msg = Message(output, Timestamp(coordinates=[0]))
                self.get_output_stream('sync_stream').send(output_msg)
            else:
                # Remove the oldest message.
                self._stream_data[min_stream_name] = self._stream_data[
                    min_stream_name][1:]

    def execute(self):
        self.spin()
