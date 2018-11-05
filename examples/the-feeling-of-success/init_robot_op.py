from std_msgs.msg import Bool

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op

import intera_interface
import rospy


class InitRobotOperator(Op):
    """
    Initializes the Sawyer arm and moves the arm to the given rest position and
    opens the gripper.
    """
    stream_name = "init-robot-stream"

    def __init__(self, name, joint_angles, limb_name):
        """
        Initializes the robot state to the given joint angles and
        connects to the Limb interface and the Gripper of the robot.
        """
        super(InitRobotOperator, self).__init__(name)
        self.joint_angles = joint_angles
        self._limb, self._gripper = None, None
        self.limb_name = limb_name

    @staticmethod
    def setup_streams(input_streams):
        """
        Registers a callback on the given input streams and returns a single
        stream which sends a Boolean value upon successful initialization.
        """
        input_streams.add_callback(InitRobotOperator.init_robot_hook)
        return [
            DataStream(
                data_type=Bool,
                name=InitRobotOperator.stream_name,
                labels={"object": "robot"})
        ]

    def init_robot_hook(self, msg):
        """
        Upon receipt of the message, move the limb to the given joint positions
        and open the gripper.
        """
        if rospy.is_shutdown():
            return
        self._limb.move_to_joint_positions(self.joint_angles, timeout=5.0)
        self._gripper.open()
        msg = Message(True, msg.timestamp)
        self.get_output_stream(InitRobotOperator.stream_name).send(msg)

    def execute(self):
        """
        Initialize the robot, the limb and the gripper.
        """
        # Initialize the limb and the robot.
        self._limb = intera_interface.Limb(self.limb_name)
        if not self._limb:
            raise AttributeError("Could not initialize the limb!")
        robot_state = intera_interface.RobotEnable(
            intera_interface.CHECK_VERSION)
        robot_state.enable()

        # Initialize the gripper.
        self._gripper = intera_interface.Gripper()
        if not self._gripper:
            raise AttributeError("Could not initialize the gripper!")
