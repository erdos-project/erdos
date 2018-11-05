from geometry_msgs.msg import Pose, Point
from gazebo_msgs.srv import SpawnModel
from std_msgs.msg import Bool

from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message

import rospkg
import rospy


class InsertBlockOperator(Op):
    """
    Inserts a Block into the currently running Gazebo environment at the given
    coordinates.
    """
    stream_name = "insert-block-stream"

    def __init__(self, name, _x, _y, _z, ref_frame):
        """
        Initialize the state of the operator.
        _x: The x-coordinate of the block.
        _y: The y-coordinate of the block.
        _z: The z-coordinate of the block.
        ref_frame: The frame in which to insert the block.
        """
        super(InsertBlockOperator, self).__init__(name)
        self.block_pose = Pose(position=Point(x=_x, y=_y, z=_z))
        self.block_reference_name = ref_frame

    @staticmethod
    def setup_streams(input_streams):
        """
        Adds the insert_block_hook callback on all the received streams, and
        returns a single stream which sends a boolean success value of the
        insertion operation.
        """
        input_streams.add_callback(InsertBlockOperator.insert_block_hook)
        return [
            DataStream(
                data_type=Bool,
                name=InsertBlockOperator.stream_name,
                labels={"object": "block"})
        ]

    def insert_block_hook(self, msg):
        """
        Retrieves the model file of the block and inserts it into the current
        Gazebo environment.
        """
        model_path = rospkg.RosPack().get_path(
            'sawyer_sim_examples') + "/models/"
        with open(model_path + "block/model.urdf", "r") as block_file:
            block_xml = block_file.read().replace('\n', '')
        rospy.wait_for_service('/gazebo/spawn_urdf_model')
        spawn_urdf = rospy.ServiceProxy('/gazebo/spawn_urdf_model', SpawnModel)
        resp_urdf = spawn_urdf("block", block_xml, "/", self.block_pose,
                               self.block_reference_name)
        output_msg = Message(True, msg.timestamp)
        self.get_output_stream(
            InsertBlockOperator.stream_name).send(output_msg)
