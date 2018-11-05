from geometry_msgs.msg import Pose, Point

from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message


class RaiseObjectOperator(Op):
    """
    Raises the Sawyer arm while gripping the object.
    """
    stream_name = "raise-object-stream"

    def __init__(self, name):
        """
        Initializes the destination coordinates of the arm.
        """
        super(RaiseObjectOperator, self).__init__(name)
        self.des_EE_xyz = None
        self.orientation = None

    @staticmethod
    def setup_streams(input_streams, location_stream_name,
                      trigger_stream_name):
        """
        Registers a callback to retrieve the location where the arm needs to
        be moved and returns a single output stream which sends the Pose
        commands to move the robot to the required location.
        """
        input_streams.filter_name(location_stream_name)\
            .add_callback(RaiseObjectOperator.save_destination)
        input_streams.filter_name(trigger_stream_name)\
            .add_callback(RaiseObjectOperator.generate_move_commands)
        return [
            DataStream(data_type=Pose, name=RaiseObjectOperator.stream_name)
        ]

    def save_destination(self, msg):
        """
        Saves the destination coordinates and orientation of the arm.
        """
        self.des_EE_xyz = msg.data.des_EE_xyz_above
        self.orientation = msg.data.des_orientation_EE

    def generate_move_commands(self, msg):
        """
        Creates the Pose object from the retrieved coordinates.
        """
        raise_object_pose = Pose(
            position=Point(
                x=self.des_EE_xyz[0],
                y=self.des_EE_xyz[1],
                z=self.des_EE_xyz[2]),
            orientation=self.orientation)
        raise_object_msg = Message(raise_object_pose, msg.timestamp)
        self.get_output_stream(RaiseObjectOperator.stream_name).\
            send(raise_object_msg)

    def execute(self):
        self.spin()
