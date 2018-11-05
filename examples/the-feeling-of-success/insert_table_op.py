from geometry_msgs.msg import Pose, Point
from gazebo_msgs.srv import SpawnModel
from std_msgs.msg import Bool

from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.timestamp import Timestamp
from erdos.message import Message

import rospkg
import rospy


class InsertTableOperator(Op):
    """
    Inserts a Table into the currently running Gazebo environment with the
    given coordinates.
    """
    stream_name = "insert-table-stream"

    def __init__(self, name, _x, _y, _z, ref_frame):
        """
        Initialize the state of the operator.
        _x: The x-coordinate of the table.
        _y: The y-coordinate of the table.
        _z: The z-coordinate of the table.
        ref_frame: The frame in which to insert the table at the given coordinates.
        """
        super(InsertTableOperator, self).__init__(name)
        self.table_pose = Pose(position=Point(x=_x, y=_y, z=_z))
        self.table_reference_frame = ref_frame

    @staticmethod
    def setup_streams(input_streams):
        """
        Returns a single datastream which sends a boolean success value of the
        insertion operation.
        """
        return [
            DataStream(
                data_type=Bool,
                name=InsertTableOperator.stream_name,
                labels={"object": "table"})
        ]

    def execute(self):
        """
        Retrieves the model file of the table and inserts it into the current
        Gazebo environment.
        """
        model_path = rospkg.RosPack().get_path(
            'sawyer_sim_examples') + "/models/"
        with open(model_path + "cafe_table/model.sdf", "r") as table_file:
            table_xml = table_file.read().replace('\n', '')

        rospy.wait_for_service('/gazebo/spawn_sdf_model')
        spawn_sdf = rospy.ServiceProxy('/gazebo/spawn_sdf_model', SpawnModel)
        resp_sdf = spawn_sdf("cafe_table", table_xml, '/', self.table_pose,
                             self.table_reference_frame)
        output_msg = Message(True, Timestamp(coordinates=[0]))
        self.get_output_stream(InsertTableOperator.stream_name).\
            send(output_msg)
        self.spin()
