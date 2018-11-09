import logging
import pickle

import rospy
from std_msgs.msg import String

from erdos.data_stream import DataStream

logger = logging.getLogger(__name__)


class ROSInputDataStream(DataStream):
    def __init__(self, op, data_stream):
        super(ROSInputDataStream, self).__init__(
            data_type=data_stream.data_type,
            name=data_stream.name,
            labels=data_stream.labels,
            callbacks=data_stream.callbacks,
            uid=data_stream.uid)
        self.op = op

    def setup(self):
        """Initializes a ROS subscriber."""
        data_type = self.data_type if self.data_type else String
        # TODO(ionel): We currently transform messages to Strings because
        # we want to pass timestamp and stream info along with the message.
        # However, the extra serialization can add overheads. Fix!
        rospy.Subscriber(self.uid, String, callback=self._on_msg)

    def _on_msg(self, msg):
        #data = msg if self.data_type else pickle.loads(msg.data)
        msg = pickle.loads(msg.data)
        for on_msg_callback in self.callbacks:
            on_msg_callback(self.op, msg)
