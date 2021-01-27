"""Sends a ros message to a subscribed operator that
converts them to an erdos message.

Uses a publisher to test whether the converting
operator is successful.
"""

from multiprocessing import Value
import time
import pytest

import rospy
from std_msgs.msg import Int64
from rospy.exceptions import ROSException

import erdos


ROSTOPIC = "test_ros_to_erdos"
ROS_NODE_NAME = "ros_to_erdos_node"
RECEIVED_MESSAGES = []
NUM_RECEIVED = Value("i", 0)
TEST_FAILED = Value("i", 0)


class RecvOp(erdos.Operator):
    """For testing purposes.

    Receives a stream of Erdos messages. Checks them against
    expected_messages.

    Args:
        read_stream: Stream from which messages are read
        expected_messages: List of Erdos objects that are
            expected to be received from the read stream.
    """

    def __init__(self, read_stream, expected_messages):
        self.read_stream = read_stream
        self.read_stream.add_callback(self.check_recvd_msg)
        self.messages = expected_messages

    @staticmethod
    def connect(read_stream):
        return []

    def check_recvd_msg(self, recvd_msg):
        print("Received Erdos Message: " + str(recvd_msg) +
              ". Expected: " + str(self.messages[self.cur_msg]) + ".")
        if recvd_msg.data != self.messages[self.cur_msg]:
            TEST_FAILED.value = True
        self.cur_msg += 1
        NUM_RECEIVED.value += 1

    def run(self):
        self.cur_msg = 0


class RosToErdosOp(erdos.Operator):
    """Converts ROS messages to Erdos messages.
    Args:
        subscribe_stream (:py:class:`erdos.WriteStream`): Stream on which the
            operator sends messages received from ROS.
        ros_msg_type: Type of ros messages sent from rostopic
        rostopic: rostopic to subscribe to
        node_name: Ros node name
        func: Callback function that converts a single ros_msg to an erdos_msg
    """

    def __init__(self, subscribe_stream, ros_msg_type, rostopic, node_name,
                 func):
        self.logger = erdos.utils.setup_logging(self.config.name,
                                                self.config.log_file_name)
        self.subscribe_stream = subscribe_stream
        self.ros_msg_type = ros_msg_type
        self.rostopic = rostopic
        self.node_name = node_name
        self.func = func
        self.coord = 0

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def on_ros_msg(self, ros_msg):
        """
        Callback for ros messages that converts
        them to erdos messages and then sends them
        on the subscribe_stream. Used by the
        operator's ros subscriber.

        ros_msg: ros message that will be converted to
            an erdos message and then published.
        """

        print("Received Ros Message: " + str(ros_msg))
        erdos_msg = self.func(ros_msg)
        erdos_msg = erdos.Message(erdos.Timestamp(coordinates=[self.coord]),
                                  erdos_msg)
        self.subscribe_stream.send(erdos_msg)
        self.coord += 1

    def run(self):
        # Initialize a subscriber
        try:
            rospy.init_node(ROS_NODE_NAME + "_operator", anonymous=True,
                            disable_signals=True)
        except ROSException as err:
            print("Rospy operator node already initialized. " +
                  "Skip initialization: " + str(err))
        rospy.Subscriber(self.rostopic, self.ros_msg_type, self.on_ros_msg)
        rospy.spin()


@pytest.fixture
def prep_globs():
    """Resets global variables to prepare for test."""

    NUM_RECEIVED.value = 0
    TEST_FAILED.value = False
    # erdos.reset()


@pytest.mark.parametrize("ros_msgs, ros_msg_type, erdos_msgs, " +
                         "sub_func, topic",
                         [
                            ([0, 1, 2, 3, 4, 5],
                             Int64,
                             ["Zero", "One", "Two", "Three", "Four", "Five"],
                             lambda msg: ["Zero", "One", "Two", "Three",
                                          "Four", "Five"][msg.data],
                             ROSTOPIC + "_1"),
                            ([0, 1, 2, 3, 4, 5],
                             Int64,
                             ["Zero", "One", "Two", "Three", "Four", "Five"],
                             lambda msg: "",
                             ROSTOPIC + "_2"),
                         ])
def test_int_str(prep_globs,
                 ros_msgs,
                 ros_msg_type,
                 erdos_msgs,
                 sub_func,
                 topic):
    """
    Test converting a ros Int64 to an erdos String.
    Test 1 should pass.
    Test 2 should fail.
    """

    pub = rospy.Publisher(topic, ros_msg_type, queue_size=10)
    (sub_stream, ) = erdos.connect(RosToErdosOp,
                                   erdos.OperatorConfig(),
                                   [],
                                   ros_msg_type=ros_msg_type,
                                   rostopic=topic,
                                   node_name=ROS_NODE_NAME,
                                   func=sub_func)
    erdos.connect(RecvOp,
                  erdos.OperatorConfig(),
                  [sub_stream],
                  expected_messages=erdos_msgs)

    handle = erdos.run_async()
    time.sleep(10)
    try:
        rospy.init_node(ROS_NODE_NAME, anonymous=True, disable_signals=True)
    except ROSException as err:
        print("Rospy node already initialized. Skip initialization: " +
              str(err))
    for msg in ros_msgs:
        pub.publish(msg)
    time.sleep(15)
    handle.shutdown()
    assert NUM_RECEIVED.value == len(erdos_msgs)
    assert not TEST_FAILED.value


# def main():
#     ros_msgs, ros_msg_type, erdos_msgs, sub_func, topic = ([0, 1, 2, 3,
#      4, 5],
#      Int64,
#      ["Zero", "One", "Two", "Three", "Four", "Five"],
#      lambda msg: ["Zero", "One", "Two", "Three", "Four", "Five"][msg.data],
#      ROSTOPIC + "_1")
#     pub = rospy.Publisher(topic, ros_msg_type, queue_size=10)
#     print(pub.name)
#     #s = rospy.Subscriber(topic, ros_msg_type, lambda x: print(x))
#     #print(s.impl.callbacks)
#     (sub_stream, ) = erdos.connect(RosToErdosOp,
#                      erdos.OperatorConfig(),
#                      [],
#                      ros_msg_type=ros_msg_type,
#                      rostopic=topic,
#                      node_name=ROS_NODE_NAME,
#                      func=sub_func)
#     erdos.connect(RecvOp,
#                   erdos.OperatorConfig(),
#                   [sub_stream],
#                   expected_messages=erdos_msgs)
#     handle = erdos.run_async()
#     #erdos.reset()
#     time.sleep(10)
#     #print(s.impl.callbacks)
#     print("Number of connections: " + str(pub.get_num_connections()))
#     rospy.init_node(ROS_NODE_NAME, anonymous=True, disable_signals=True)
#     for msg in ros_msgs:
#         pub.publish(msg)
#     #rospy.spin()
#     time.sleep(10)
#     handle.shutdown()
#     assert NUM_RECEIVED.value == len(erdos_msgs)
#     assert not TEST_FAILED.value

# if __name__ == '__main__':
#     main()
