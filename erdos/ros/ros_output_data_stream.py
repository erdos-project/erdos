import logging
import pickle
import time

import rospy
from std_msgs.msg import String

from erdos.data_stream import DataStream

logger = logging.getLogger(__name__)


class ROSOutputDataStream(DataStream):
    def __init__(self, data_stream):
        super(ROSOutputDataStream, self).__init__(
            data_type=data_stream.data_type,
            name=data_stream.name,
            labels=data_stream.labels,
            callbacks=data_stream.callbacks,
            uid=data_stream.uid)
        self.publisher = None

    def send(self, msg):
        """Sending a message on a ROS stream (i.e., publishes it)."""
        msg.stream_name = self.name
        msg = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
        self.publisher.publish(msg)

    def setup(self):
        """Setups the source operator as a publisher."""
        data_type = self.data_type if self.data_type else String
        # TODO(ionel): We currently transform messages to Strings because
        # we want to pass timestamp and stream info along with the message.
        # However, the extra serialization can add overheads. Fix!

        # queue_size = None ensures that the subscriber is blocking in case
        # its queue is full (i.e., when the operator publishes more data than
        # it can be serialized and sent over the network).
        self.publisher = rospy.Publisher(
            self.uid, String, latch=True, queue_size=None)
        # TODO(yika): hacky way to stall generator publisher in order to wait
        # for all other processes finish initiating
        time.sleep(1)
