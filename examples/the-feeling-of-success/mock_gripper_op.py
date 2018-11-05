from collections import namedtuple
from std_msgs.msg import Bool
import intera_interface

from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message

MockGripType = namedtuple("MockGripType", "action")


class MockGripperOperator(Op):
    """
    Interface to the gripper.
    Retrieves the action from the message and executes the action.
    """

    def __init__(self, name, gripper_speed, output_stream_name):
        """
        Initializes the gripper with the given speed.
        """
        super(MockGripperOperator, self).__init__(name)
        self.gripper_speed = gripper_speed
        self._gripper = None
        self.output_stream_name = output_stream_name

    @staticmethod
    def setup_streams(input_streams, gripper_stream, output_stream_name):
        """
        Registers callback on the given stream and publishes a completion
        message on the given output stream name.
        """
        input_streams.filter_name(gripper_stream).\
            add_callback(MockGripperOperator.gripmove)
        return [DataStream(data_type=Bool, name=output_stream_name)]

    def gripmove(self, msg):
        """
        Retrieves the given action from the message and executes the given
        action.
        """
        if msg.data.action == "open":
            self._gripper.open()
        elif msg.data.action == "close":
            self._gripper.close()
        action_complete_msg = Message(True, msg.timestamp)
        self.get_output_stream(self.output_stream_name).\
            send(action_complete_msg)

    def execute(self):
        """
        Initializes a connection to the gripper.
        """
        self._gripper = intera_interface.Gripper()
        rs = intera_interface.RobotEnable(intera_interface.CHECK_VERSION)
        rs.enable()
        self.spin()
