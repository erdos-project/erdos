import rospy

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp


class ROSSubscriberOp(Op):
    """Bridges between ROS topics and ERDOS streams.

    Publishes each message received on a ROS topic to an ERDOS data stream.

    Attributes:
        ros_topics_type: A list of (ros_topic_name, type)
    """

    def __init__(self, name, ros_topics_type):
        super(ROSSubscriberOp, self).__init__(name)
        self._ros_topics_type = ros_topics_type
        self.name_mappings = {}
        for (ros_topic, data_type, output_topic) in ros_topics_type:
            self.name_mappings[ros_topic] = output_topic

    @staticmethod
    def setup_streams(input_streams, ros_topics_type):
        """Constructs ERDOS data streams for each provided ROS topic.

        Args:
            input_streams (DataStreams): Operator does not subscribe to any
                data streams.
            ros_topics_type (list): Descriptions of ROS topics for which to
                generate data streams. Elements of the list must be tuples
                of (ros_topic, data_type, output_topic).

        Returns:
            (list of DataStream): data stream for each provided ROS topic.
        """
        output_streams = []
        for (ros_topic, data_type, output_topic) in ros_topics_type:
            output_streams.append(
                DataStream(data_type=data_type, name=output_topic))
        return output_streams

    def on_next(self, data):
        """Called for each message received on one of the topics"""
        msg = Message(data, Timestamp(coordinates=[0]))
        topic = self.name_mappings[data._connection_header['topic']]
        self.get_output_stream(topic).send(msg)

    def execute(self):
        if self.framework != 'ros':
            rospy.init_node(self.name, anonymous=True)
        for (ros_topic, data_type, _) in self._ros_topics_type:
            rospy.Subscriber(ros_topic, data_type, self.on_next)
        rospy.spin()
