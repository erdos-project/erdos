import rospy

from erdos.op import Op


class ROSPublisherOp(Op):
    """Bridges between ERDOS streams and ROS topics.

    Publishes each message received on an ERDOS data stream to a ROS topic.

    Attributes:
        ros_topic: The name of the topic to publish to.
        data_type: The type of the topic data.
    """

    def __init__(self, name, ros_topic, data_type):
        super(ROSPublisherOp, self).__init__(name)
        self._ros_topic = ros_topic
        self._data_type = data_type
        self._publisher = None

    @staticmethod
    def setup_streams(input_streams):
        """Subscribes to all ERDOS data streams and forwards messages to ROS"""
        input_streams.add_callback(ROSPublisherOp.on_next)
        return []

    def on_next(self, msg):
        """Publishes each message received on the ERDOS stream to ROS"""
        self._publisher.publish(msg.data)

    def execute(self):
        if self.framework != 'ros':
            rospy.init_node(self._ros_topic, anonymous=True)
        self._publisher = rospy.Publisher(
            self._ros_topic, self._data_type, queue_size=10)
        self.spin()
