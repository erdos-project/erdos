import os
import sys

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency

from std_msgs.msg import String


class SynchronizerBestDataOperator(Op):
    """Publishes messages at a given frequency. Each time it publishes the
       best synchronzied data it received since last invocation.
    """

    def __init__(self, name='synchronizer'):
        super(SynchronizerBestDataOperator, self).__init__(name)
        self._best_sync = {}
        self._epoch = 0

    @staticmethod
    def setup_streams(input_streams):
        # self.input_streams['loop_iter'].register(self.on_loop_complete)
        input_streams.add_callback(
            SynchronizerBestDataOperator.on_msg_best_sync_data)
        return [DataStream(data_type=String, name='sync_stream')]

    def on_loop_complete(self, msg):
        """Publishes best synced data received while the loop was running."""
        self._epoch += 1
        timestamp = Timestamp(coordinates=[self._epoch - 1])
        msg = Message(self._best_sync[self._epoch - 1], timestamp)
        self.get_output_stream('sync_stream').send(msg)
        del self._best_sync[self._epoch - 1]

    @frequency(1)
    def on_msg_best_sync_data_at_freq(self):
        """Publishes best synced data received since last invocation."""
        self._epoch += 1
        timestamp = Timestamp(coordinates=[self._epoch - 1])
        msg = Message(self._best_sync[self._epoch - 1], timestamp)
        self.get_output_stream('sync_stream').send(msg)
        del self._best_sync[self._epoch - 1]

    def on_msg_best_sync_data(self, msg):
        if self._epoch not in self._best_sync:
            self._best_sync[self._epoch] = {}
        # TODO(ionel): We don't currently compute the best sync messages.
        # We just override messages with the last received messages.
        self._best_sync[self._epoch][msg.stream_name] = msg

    def execute(self):
        self.on_msg_best_sync_data_at_freq()
        self.spin()
