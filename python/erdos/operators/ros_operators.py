"""Operators that interface between ros nodes and erdos operators."""

import rospy
from rospy.exceptions import ROSException, ROSSerializationException

import erdos


class ErdosToRosOp(erdos.Operator):
    """Converts Erdos messages to ROS messages.
    Args:
        erdos_stream (:py:class:`erdos.ReadStream`): Stream on which the
            operator receives control commands.
        ros_msg_type: Type used to publish to ros messages
        ros_topic: ros topic to publish messages to
        node_name: Ros node name
        conversion_func: Callback fn that converts a ros_msg to an erdos_msg
        ros_queue_size: Queue size of the ros publisher
    """
    def __init__(self, erdos_stream, ros_msg_type, ros_topic, node_name,
                 conversion_func, ros_queue_size=10):
        self.logger = erdos.utils.setup_logging(self.config.name,
                                                self.config.log_file_name)
        self.erdos_stream = erdos_stream
        self.erdos_stream.add_callback(self.on_erdos_msg)
        self.ros_msg_type = ros_msg_type
        self.ros_topic = ros_topic
        self.node_name = node_name
        self.conversion_func = conversion_func
        self.ros_queue_size = ros_queue_size
        self.ros_pub = None

    @staticmethod
    def connect(read_streams):
        return []

    def on_erdos_msg(self, msg):
        """
        Callback for erdos_stream that converts
        erdos messages to ros messages and then
        published them.

        msg: Erdos message that will be converted
            to ros message and then published.
        """

        new_ros_msg = self.conversion_func(msg)
        self.logger.debug("Publishing: " + str(new_ros_msg))
        try:
            self.ros_pub.publish(new_ros_msg)
        except (NameError, AttributeError) as err:
            self.logger.debug(
                "Ros publisher was not defined, \
                possibly because run() was not called: %s", err)
        except (ROSException, ROSSerializationException) as err:
            self.logger.debug("The erdos msg couldn't publish to ros: %s", err)

    def run(self):
        # Initialize a publisher
        self.ros_pub = rospy.Publisher(self.ros_topic,
                                       self.ros_msg_type,
                                       queue_size=self.ros_queue_size)
        rospy.init_node(self.node_name, anonymous=True, disable_signals=True)


class RosToErdosOp(erdos.Operator):
    """Converts ROS messages to Erdos messages.
    Args:
        ros_stream (:py:class:`erdos.WriteStream`): Stream on which the
            operator sends messages received from ROS.
        ros_msg_type: Type of ros messages sent from ros_topic
        ros_topic: ros_topic to subscribe to
        node_name: Ros node name
        conversion_func: Callback function that converts a single ros_msg
            to an erdos_msg
    """
    def __init__(self, ros_stream, ros_msg_type, ros_topic, node_name,
                 conversion_func):
        self.logger = erdos.utils.setup_logging(self.config.name,
                                                self.config.log_file_name)
        self.ros_stream = ros_stream
        self.ros_msg_type = ros_msg_type
        self.ros_topic = ros_topic
        self.node_name = node_name
        self.conversion_func = conversion_func
        self.coord = 0

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def on_ros_msg(self, ros_msg):
        """
        Callback for ros messages that converts
        them to erdos messages and then sends them
        on the ros_stream. Used by the
        operator's ros subscriber.

        ros_msg: ros message that will be converted to
            an erdos message and then published.
        """

        self.logger.debug("Received Ros Message: " + str(ros_msg))
        erdos_msg = self.conversion_func(ros_msg)
        erdos_msg = erdos.Message(erdos.Timestamp(coordinates=[self.coord]),
                                  erdos_msg)
        self.ros_stream.send(erdos_msg)
        self.coord += 1

    def run(self):
        # Initialize a subscriber
        try:
            rospy.init_node(self.node_name,
                            anonymous=True,
                            disable_signals=True)
        except ROSException as err:
            self.logger.debug("Rospy operator node already initialized. " +
                  "Skip initialization: " + str(err))
        rospy.Subscriber(self.ros_topic, self.ros_msg_type, self.on_ros_msg)
        rospy.spin()
