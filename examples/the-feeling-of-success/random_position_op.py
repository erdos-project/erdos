from geometry_msgs.msg import Pose, Point, Quaternion
from std_msgs.msg import Bool
import numpy as np
import rospy
import intera_interface

from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message


class RandomPositionOperator(Op):
    """
    Retrieves a random location and generates Pose messages to move the arm
    to the given location.
    """
    position_stream_name = "random-position-stream"
    action_complete_stream_name = "random-position-complete-stream"

    def __init__(self, name):
        """
        Initializes variables to save the destination location and locks.
        """
        super(RandomPositionOperator, self).__init__(name)
        self.des_EE_xyz, self.orientation = None, None
        self.move_ahead_lock = True
        self.limb = None

    @staticmethod
    def setup_streams(input_streams, locate_object_stream_name,
                      trigger_stream_name, goto_xyz_stream_name):
        """
        Register callbacks to retrieve the destination locations and to wait
        for completion of previously sent actions.
        Return two streams. Send the Pose commands on the first and the
        message completion command on the second.
        """
        input_streams.filter_name(locate_object_stream_name) \
            .add_callback(RandomPositionOperator.save_coords)
        input_streams.filter_name(trigger_stream_name) \
            .add_callback(RandomPositionOperator.generate_random_position)
        input_streams.filter_name(goto_xyz_stream_name)\
            .add_callback(RandomPositionOperator.release_lock)
        return [
            DataStream(
                data_type=Pose,
                name=RandomPositionOperator.position_stream_name),
            DataStream(
                data_type=Bool,
                name=RandomPositionOperator.action_complete_stream_name)
        ]

    def save_coords(self, msg):
        """
        Save the destination coordinates.
        """
        self.des_EE_xyz = msg.data.des_EE_xyz
        self.orientation = msg.data.des_orientation_EE

    def release_lock(self, msg):
        """
        Release the lock so future actions can be sent to the robot.
        """
        self.move_ahead_lock = True

    def generate_random_position(self, msg):
        """
        Generate the Pose commands to move the arm to the given location.
        """
        # Move the arm to the object slowly.
        steps = 400.0
        time = 4.0
        r = rospy.Rate(1 / (time / steps))
        current_pose = self.limb.endpoint_pose()
        ik_delta = Pose()
        pose = Pose(
            position=Point(
                x=self.des_EE_xyz[0],
                y=self.des_EE_xyz[1],
                z=self.des_EE_xyz[2]),
            orientation=self.orientation)
        ik_delta.position.x = (
            current_pose['position'].x - pose.position.x) / steps
        ik_delta.position.y = (
            current_pose['position'].y - pose.position.y) / steps
        ik_delta.position.z = (
            current_pose['position'].z - pose.position.z) / steps
        ik_delta.orientation.x = (
            current_pose['orientation'].x - pose.orientation.x) / steps
        ik_delta.orientation.y = (
            current_pose['orientation'].y - pose.orientation.y) / steps
        ik_delta.orientation.z = (
            current_pose['orientation'].z - pose.orientation.z) / steps
        ik_delta.orientation.w = (
            current_pose['orientation'].w - pose.orientation.w) / steps
        for d in range(int(steps), -1, -1):
            if rospy.is_shutdown():
                return
            ik_step = Pose()
            ik_step.position.x = d * ik_delta.position.x + pose.position.x
            ik_step.position.y = d * ik_delta.position.y + pose.position.y
            ik_step.position.z = d * ik_delta.position.z + pose.position.z
            ik_step.orientation.x = d * ik_delta.orientation.x + pose.orientation.x
            ik_step.orientation.y = d * ik_delta.orientation.y + pose.orientation.y
            ik_step.orientation.z = d * ik_delta.orientation.z + pose.orientation.z
            ik_step.orientation.w = d * ik_delta.orientation.w + pose.orientation.w
            joint_angles = self.limb.ik_request(ik_step, "right_gripper_tip")
            if joint_angles:
                joint_angle_msg = Message(ik_step, msg.timestamp)
                self.move_ahead_lock = False
                self.get_output_stream(
                    RandomPositionOperator.position_stream_name).send(
                        joint_angle_msg)
                while not self.move_ahead_lock:
                    pass
            else:
                r.sleep()

        final_msg = Message(True, msg.timestamp)
        self.get_output_stream(
            RandomPositionOperator.action_complete_stream_name).send(final_msg)

    def execute(self):
        """
        Initialize the connection to the limb.
        """
        self.limb = intera_interface.Limb('right')
        rs = intera_interface.RobotEnable(intera_interface.CHECK_VERSION)
        rs.enable()
        self.spin()
