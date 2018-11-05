from orientations import Orientations
from geometry_msgs.msg import Pose, Point
from std_msgs.msg import Bool

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op

import rospy
import intera_interface


class MoveAboveObjectOperator(Op):
    """
    Moves the given Sawyer arm over the detected object.
    """
    goto_stream_name = "goto-stream"
    stream_name = "grip-stream"

    def __init__(self, name):
        """
        Initializes the lock variable and the limb.
        """
        super(MoveAboveObjectOperator, self).__init__(name)
        self.move_ahead_lock = True
        self.limb = None

    @staticmethod
    def setup_streams(input_streams, trigger_stream_name,
                      goto_xyz_stream_name):
        """
        Takes as input the stream that releases the lock upon successfull
        completion of the given action and returns the output stream which
        publishes the Pose to move the arm to.
        """
        input_streams.filter_name(trigger_stream_name)\
            .add_callback(MoveAboveObjectOperator.move_object)
        input_streams.filter_name(goto_xyz_stream_name)\
            .add_callback(MoveAboveObjectOperator.release_lock)
        return [
            DataStream(
                data_type=Pose, name=MoveAboveObjectOperator.goto_stream_name),
            DataStream(
                data_type=Bool, name=MoveAboveObjectOperator.stream_name)
        ]

    def move_object(self, msg):
        """
        Moves the object over the given location.
        """
        # First move the arm over the object.
        move_above_pose = Pose(
            position=Point(
                x=msg.data.des_EE_xyz_above[0],
                y=msg.data.des_EE_xyz_above[1],
                z=msg.data.des_EE_xyz_above[2]),
            orientation=Orientations.DOWNWARD_ROTATED)
        move_above_msg = Message(move_above_pose, msg.timestamp)
        self.move_ahead_lock = False
        self.get_output_stream(MoveAboveObjectOperator.goto_stream_name).\
            send(move_above_msg)
        while not self.move_ahead_lock:
            pass

        # Now rotate the gripper.
        rotate_gripper_pose = Pose(
            position=Point(
                x=msg.data.des_EE_xyz_above[0],
                y=msg.data.des_EE_xyz_above[1],
                z=msg.data.des_EE_xyz_above[2]),
            orientation=msg.data.des_orientation_EE)
        rotate_gripper_msg = Message(rotate_gripper_pose, msg.timestamp)
        self.move_ahead_lock = False
        self.get_output_stream(MoveAboveObjectOperator.goto_stream_name).\
            send(rotate_gripper_msg)
        while not self.move_ahead_lock:
            pass

        # Move the arm to the object slowly.
        steps = 400.0
        time = 4.0
        r = rospy.Rate(1 / (time / steps))
        current_pose = self.limb.endpoint_pose()
        ik_delta = Pose()
        pose = Pose(
            position=Point(
                x=msg.data.des_EE_xyz[0],
                y=msg.data.des_EE_xyz[1],
                z=msg.data.des_EE_xyz[2]),
            orientation=msg.data.des_orientation_EE)
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
            ik_step.orientation.x = d * ik_delta.orientation.x + \
                pose.orientation.x
            ik_step.orientation.y = d * ik_delta.orientation.y + \
                pose.orientation.y
            ik_step.orientation.z = d * ik_delta.orientation.z + \
                pose.orientation.z
            ik_step.orientation.w = d * ik_delta.orientation.w + \
                pose.orientation.w
            joint_angles = self.limb.ik_request(ik_step, "right_gripper_tip")
            if joint_angles:
                joint_angle_msg = Message(ik_step, msg.timestamp)
                self.move_ahead_lock = False
                self.get_output_stream(MoveAboveObjectOperator.\
                                       goto_stream_name).send(joint_angle_msg)
                while not self.move_ahead_lock:
                    pass
            else:
                r.sleep()

        ## Initiate the gripper.
        action_complete_msg = Message(True, msg.timestamp)
        self.get_output_stream(MoveAboveObjectOperator.stream_name).\
            send(action_complete_msg)

    def release_lock(self, msg):
        """
        Release the lock when the action is complete.
        """
        self.move_ahead_lock = True

    def execute(self):
        """
        Initializes the connection to the limb.
        """
        self.limb = intera_interface.Limb('right')
        rs = intera_interface.RobotEnable(intera_interface.CHECK_VERSION)
        rs.enable()
        self.spin()
