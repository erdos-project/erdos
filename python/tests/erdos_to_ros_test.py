"""Sends an erdos message to an operator that converts them
to a ros message and publishes them.

Uses a subscriber to test whether the converting operator
is successful.

This test requires a ros master node running. Launch one
with the 'roscore' command before running the test.
"""

from multiprocessing import Value
import time
import pytest

import rospy
from std_msgs.msg import String

import erdos

from erdos.operators.ros_operators import ErdosToRosOp

ROS_TOPIC = "test_erdos_to_ros"
ROS_NODE_NAME = "erdos_to_ros_node"
RECEIVED_MESSAGES = []
NUM_RECEIVED = Value("i", 0)
TEST_FAILED = Value("i", 0)


class SendOp(erdos.Operator):
    """Generates and sends a stream of Erdos messages.
    Args:
        write_stream: Stream to which messages are written
        messages: List of Python primitives or objects that
            are sent as Erdos messages in the write stream.
    """
    def __init__(self, write_stream, messages):
        self.write_stream = write_stream
        self.messages = messages
        self.coord = 0

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        for raw_msg in self.messages:
            msg = erdos.Message(erdos.Timestamp(coordinates=[self.coord]),
                                raw_msg)
            print("SendOp: sending {msg}".format(msg=msg))
            self.write_stream.send(msg)
            self.coord += 1
            time.sleep(0.25)


def pub_helper(msg, translator):
    """Takes msg, calls the translator function on it, and returns result."""

    ros_msg = translator(msg.data)
    return ros_msg


def sub_helper(msg, verifier):
    """
    Takes msg, appends it to RECEIVED_MESSAGES,
    calls the verifier function on RECEIVED_MESSAGES,
    and updates the TEST_FAILED value.
    """

    print("Received: " + str(msg))
    RECEIVED_MESSAGES.append(msg.data)
    NUM_RECEIVED.value += 1
    TEST_FAILED.value = not verifier(RECEIVED_MESSAGES)


@pytest.fixture
def prep_globs():
    """Resets global variables to prepare for test."""

    RECEIVED_MESSAGES.clear()
    NUM_RECEIVED.value = 0
    TEST_FAILED.value = False


@pytest.mark.parametrize(
    "pub_func, sub_func, msgs, pub_msg_type, translator, verifier, topic, " +
    "pass_expected", [
        (pub_helper, sub_helper, [0, 1, 2, 3, 4, 5], String,
         lambda msg: ["Zero", "One", "Two", "Three", "Four", "Five"][msg],
         lambda rcvd: rcvd == ["Zero", "One", "Two", "Three", "Four", "Five"
                               ][:len(rcvd)], ROS_TOPIC + "_1", True),
        (pub_helper, sub_helper, [0, 1, 2, 3, 4, 5], String,
         lambda msg: ["Zero", "One", "Two", "Three", "Four", "Five"][msg],
         lambda received: False, ROS_TOPIC + "_2", False),
    ])
def test_int_str(prep_globs, pub_func, sub_func, msgs, pub_msg_type,
                 translator, verifier, topic, pass_expected):
    """
    Test converting an erdos int to a ros String.
    Test 1 should pass.
    Test 2 should fail.
    """

    (count_stream, ) = erdos.connect(SendOp,
                                     erdos.OperatorConfig(), [],
                                     messages=msgs)
    erdos.connect(ErdosToRosOp,
                  erdos.OperatorConfig(), [count_stream],
                  ros_msg_type=pub_msg_type,
                  ros_topic=topic,
                  node_name=ROS_NODE_NAME,
                  conversion_func=lambda msg: pub_func(msg, translator))
    rospy.Subscriber(topic, pub_msg_type, lambda msg: sub_func(msg, verifier))
    handle = erdos.run_async()
    erdos.reset()
    time.sleep(10)
    handle.shutdown()
    assert NUM_RECEIVED.value == len(msgs)
    assert pass_expected == (not TEST_FAILED.value)
