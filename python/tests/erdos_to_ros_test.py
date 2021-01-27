"""Sends a erdos message to an operator that converts them
to a ros message and publishes them.

Uses a subscriber to test whether the converting operator
is successful.
"""

from multiprocessing import Value
import time
import pytest

import rospy
from std_msgs.msg import String
from rospy.exceptions import ROSException, ROSSerializationException

import erdos


ROSTOPIC = "test_erdos_to_ros"
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


class ErdosToRosOp(erdos.Operator):
    """Converts Erdos messages to ROS messages.
    Args:
        publish_stream (:py:class:`erdos.ReadStream`): Stream on which the
            operator receives control commands.
        ros_msg_type: Type used to publish to ros messages
        rostopic: rostopic to publish messages to
        node_name: Ros node name
        func: Callback function that converts a single ros_msg to an erdos_msg
    """

    def __init__(self, publish_stream, ros_msg_type, rostopic, node_name,
                 func):
        self.logger = erdos.utils.setup_logging(self.config.name,
                                                self.config.log_file_name)
        self.publish_stream = publish_stream
        self.publish_stream.add_callback(self.on_erdos_msg)
        self.ros_msg_type = ros_msg_type
        self.rostopic = rostopic
        self.node_name = node_name
        self.func = func
        self.ros_pub = None

    @staticmethod
    def connect(read_streams):
        return []

    def on_erdos_msg(self, msg):
        """
        Callback for publish_stream that converts
        erdos messages to ros messages and then
        published them.

        msg: Erdos message that will be converted
            to ros message and then published.
        """

        new_ros_msg = self.func(msg)
        print("Publishing: " + str(new_ros_msg))
        try:
            self.ros_pub.publish(new_ros_msg)
        except (NameError, AttributeError) as err:
            self.logger.debug("Ros publisher was not defined, \
                possibly because run() was not called: %s", err)
        except (ROSException, ROSSerializationException) as err:
            self.logger.debug("The erdos msg couldn't publish to ros: %s", err)

    def run(self):
        # Initialize a publisher
        self.ros_pub = rospy.Publisher(self.rostopic, self.ros_msg_type,
                                       queue_size=10)
        rospy.init_node(self.node_name, anonymous=True, disable_signals=True)


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


@pytest.mark.parametrize("pub_func, sub_func, msgs, pub_msg_type, " + 
    "translator, verifier, topic", [
        (pub_helper,
        sub_helper,
        [0, 1, 2, 3, 4, 5],
        String,
        lambda msg: ["Zero", "One", "Two", "Three", "Four", "Five"][msg],
        lambda received: received == ["Zero", "One", "Two", "Three",
                                    "Four", "Five"][:len(received)],
        ROSTOPIC + "_1"),
        (pub_helper,
        sub_helper,
        [0, 1, 2, 3, 4, 5],
        String,
        lambda msg: ["Zero", "One", "Two", "Three", "Four", "Five"][msg],
        lambda received: False,
        ROSTOPIC + "_2"),
])
def test_int_str(prep_globs,
                 pub_func,
                 sub_func,
                 msgs,
                 pub_msg_type,
                 translator,
                 verifier,
                 topic):
    """
    Test converting an erdos int to a ros String.
    Test 1 should pass.
    Test 2 should fail.
    """

    (count_stream, ) = erdos.connect(SendOp,
                                     erdos.OperatorConfig(),
                                     [],
                                     messages=msgs)
    erdos.connect(ErdosToRosOp,
                  erdos.OperatorConfig(),
                  [count_stream],
                  ros_msg_type=pub_msg_type,
                  rostopic=topic,
                  node_name=ROS_NODE_NAME,
                  func=lambda msg: pub_func(msg, translator))
    rospy.Subscriber(topic, pub_msg_type, lambda msg: sub_func(msg, verifier))
    handle = erdos.run_async()
    erdos.reset()
    time.sleep(10)
    handle.shutdown()
    assert NUM_RECEIVED.value == len(msgs)
    assert not TEST_FAILED.value
