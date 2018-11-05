from mock_gripper_op import MockGripType
from std_msgs.msg import Bool

from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message


class MockUngraspObjectOperator(Op):
    """
    Sends an "open" action to the gripper.
    """
    gripper_stream = "ungrasp-gripper-output-stream"
    action_complete_stream_name = "ungrasp-action-complete-stream"

    def __init__(self, name):
        """
        Initializes a lock which blocks future actions to be sent until the
        past actions are completed.
        """
        super(MockUngraspObjectOperator, self).__init__(name)
        self.move_ahead_lock = True

    @staticmethod
    def setup_streams(input_streams, trigger_stream_name, gripper_stream_name):
        """
        Registers callbacks on the given streams and returns two streams, one
        of which sends actions to the gripper and the other sends a message
        upon completion of the action.
        """
        input_streams.filter_name(trigger_stream_name)\
            .add_callback(MockUngraspObjectOperator.grasp_object)
        input_streams.filter_name(gripper_stream_name)\
            .add_callback(MockUngraspObjectOperator.release_lock)
        return [
            DataStream(
                data_type=MockGripType, name=MockUngraspObjectOperator.\
                gripper_stream),
            DataStream(data_type=Bool, name=MockUngraspObjectOperator.\
                       action_complete_stream_name)
        ]

    def grasp_object(self, msg):
        """
        Send a close action to the gripper.
        """
        mock_grasp_object = MockGripType("open")
        mock_grasp_msg = Message(mock_grasp_object, msg.timestamp)
        self.move_ahead_lock = False
        self.get_output_stream(MockUngraspObjectOperator.gripper_stream).\
            send(mock_grasp_msg)
        while not self.move_ahead_lock:
            pass

        action_complete_msg = Message(True, msg.timestamp)
        self.get_output_stream(MockUngraspObjectOperator.\
                               action_complete_stream_name).\
            send(action_complete_msg)

    def release_lock(self, msg):
        """
        Release the lock so future actions can be sent.
        """
        self.move_ahead_lock = True

    def execute(self):
        self.spin()
