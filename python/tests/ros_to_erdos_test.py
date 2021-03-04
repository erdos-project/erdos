"""Sends a ros message to a subscribed operator that
converts them to an erdos message.

Uses a publisher to test whether the converting
operator is successful.

This test requires a ros master node running. Launch one
with the 'roscore' command before running the test.
"""

from multiprocessing import Value
import time
import pytest

import rospy
from std_msgs.msg import Int64
from rospy.exceptions import ROSException

import erdos

import sys
from ros_operators import ErdosToRosOp
sys.path.insert(0, "../erdos/operators")

ROS_TOPIC = "test_ros_to_erdos"
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
        print("Received Erdos Message: " + str(recvd_msg) + ". Expected: " +
              str(self.messages[self.cur_msg]) + ".")
        if recvd_msg.data != self.messages[self.cur_msg]:
            TEST_FAILED.value = True
        self.cur_msg += 1
        NUM_RECEIVED.value += 1

    def run(self):
        self.cur_msg = 0


@pytest.fixture
def prep_globs():
    """Resets global variables to prepare for test."""

    NUM_RECEIVED.value = 0
    TEST_FAILED.value = False


@pytest.mark.parametrize(
    "ros_msgs, ros_msg_type, erdos_msgs, sub_func, topic, pass_expected", [
        ([0, 1, 2, 3, 4, 5
          ], Int64, ["Zero", "One", "Two", "Three", "Four", "Five"],
         lambda msg: ["Zero", "One", "Two", "Three", "Four", "Five"][msg.data],
         ROS_TOPIC + "_1", True),
        ([0, 1, 2, 3, 4, 5], Int64, [
            "Zero", "One", "Two", "Three", "Four", "Five"
        ], lambda msg: "", ROS_TOPIC + "_2", False)
    ])
def test_int_str(prep_globs, ros_msgs, ros_msg_type, erdos_msgs, sub_func,
                 topic, pass_expected):
    """
    Test converting a ros Int64 to an erdos String.
    Test 1 should pass.
    Test 2 should fail.
    """

    pub = rospy.Publisher(topic, ros_msg_type, queue_size=10)
    (sub_stream, ) = erdos.connect(RosToErdosOp,
                                   erdos.OperatorConfig(), [],
                                   ros_msg_type=ros_msg_type,
                                   ros_topic=topic,
                                   node_name=ROS_NODE_NAME,
                                   conversion_func=sub_func)
    erdos.connect(RecvOp,
                  erdos.OperatorConfig(), [sub_stream],
                  expected_messages=erdos_msgs)

    handle = erdos.run_async()
    time.sleep(10)
    try:
        rospy.init_node(ROS_NODE_NAME + "_test", anonymous=True,
                        disable_signals=True)
    except ROSException as err:
        print("Rospy node already initialized. Skip initialization: " +
              str(err))
    for msg in ros_msgs:
        pub.publish(msg)
    time.sleep(15)
    handle.shutdown()
    assert NUM_RECEIVED.value == len(erdos_msgs)
    assert pass_expected == (not TEST_FAILED.value)
