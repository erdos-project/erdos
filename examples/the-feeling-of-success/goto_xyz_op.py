import logging
from geometry_msgs.msg import Pose, PoseStamped
import numpy as np
import time
import rospy
import intera_interface
from intera_core_msgs.srv import (
    SolvePositionIK,
    SolvePositionIKRequest,
)
from std_msgs.msg import Header, Bool
from sensor_msgs.msg import JointState

from erdos.message import Message
from erdos.op import Op
from erdos.data_stream import DataStream

logger = logging.getLogger(__name__)


class GoToXYZOperator(Op):
    """
    Class to move the Sawyer arm to the given Pose.
    """

    def __init__(self, name, limb_name="", output_stream_name=""):
        """
        Initializes the limb and the output stream name.
        """
        super(GoToXYZOperator, self).__init__(name)
        self.limb_name = limb_name
        self.limb = None
        self.output_stream_name = output_stream_name

    @staticmethod
    def setup_streams(input_streams, input_stream_name, output_stream_name):
        """
        Registers the callback for the given input stream and publishes an
        output stream on the given name.
        """
        input_streams.filter_name(input_stream_name).\
            add_callback(GoToXYZOperator.move_joint_hook)
        return [DataStream(data_type=Bool, name=output_stream_name)]

    def stamp_pose(self, pose):
        """
        Add the current timestamp to the given Pose.
        """
        hdr = Header(stamp=rospy.Time.now(), frame_id='base')
        p = PoseStamped(header=hdr, pose=pose)
        return p

    def get_joint_angles(self, pose, seed_cmd):
        """
        Convert the Pose to joint angles.
        """
        ns = "ExternalTools/" + self.limb_name +\
            "/PositionKinematicsNode/IKService"
        iksvc = rospy.ServiceProxy(ns, SolvePositionIK)
        ikreq = SolvePositionIKRequest()

        stamped_pose = self.stamp_pose(pose)
        ikreq.pose_stamp.append(stamped_pose)
        ikreq.tip_names.append('right_hand')
        ikreq.seed_mode = ikreq.SEED_USER
        seed = JointState()
        seed.name = seed_cmd.keys()
        seed.position = seed_cmd.values()
        ikreq.seed_angles.append(seed)
        try:
            rospy.wait_for_service(ns, 5.0)
            resp = iksvc(ikreq)
        except (rospy.ServiceException, rospy.ROSException), e:
            return False

        if (resp.result_type[0] > 0):
            limb_joints = dict(
                zip(resp.joints[0].name, resp.joints[0].position))
        else:
            limb_joints = None
        return limb_joints

    def move_joint_hook(self, msg):
        """
        Request the current pose from the robot and convert the given Pose to
        the joint angles required to move to that position.
        """
        joint_angles = self.limb.ik_request(msg.data, 'right_gripper_tip')
        self.limb.move_to_joint_positions(joint_angles, timeout=2)
        output_msg = Message(True, msg.timestamp)
        self.get_output_stream(self.output_stream_name).send(output_msg)

    def execute(self):
        """
        Initializes the connection to the robot.
        """
        robot_params = intera_interface.RobotParams()
        valid_limbs = robot_params.get_limb_names()
        if not valid_limbs:
            logger.error("Cannot detect any limb parameters on this robot!")
            return
        rs = intera_interface.RobotEnable(intera_interface.CHECK_VERSION)
        init_state = rs.state().enabled

        def _shutdown_hook():
            logger.info("Exiting the application ...")
            if not init_state:
                logger.info("Disabling robot ...")
                rs.disable()

        rospy.on_shutdown(_shutdown_hook)
        logger.info("Enabling robot ...")
        rs.enable()

        if self.limb_name not in valid_limbs:
            logger.error("{} is not a valid limb on this robot!".format(
                self.limb_name))

        # Initialize the limb and send it to the resting position.
        self.limb = intera_interface.Limb(self.limb_name)
        self.limb.set_joint_position_speed(0.35)
        self.spin()
