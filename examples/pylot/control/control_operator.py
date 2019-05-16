from erdos.data_stream import DataStream
from erdos.op import Op
from erdos.utils import setup_logging

from control.messages import ControlMessage
import simulation.planner.planner_operator

# Constants Used for the high level commands
REACH_GOAL = 0.0
GO_STRAIGHT = 5.0
TURN_RIGHT = 4.0
TURN_LEFT = 3.0
LANE_FOLLOW = 2.0


class ControlOperator(Op):
    def __init__(self, name, log_file_name=None):
        super(ControlOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(ControlOperator.compute_action)
        return [DataStream(name='action_stream')]

    def compute_action(self, msg):
        steer = 0.0
        throttle = 0.0
        brake = 0.0
        hand_brake = False
        reverse = False
        if msg.data == REACH_GOAL:
            brake = 0.5
        elif msg.data == GO_STRAIGHT or msg.data == LANE_FOLLOW:
            throttle = 0.5
        elif msg.data == TURN_RIGHT:
            steer = 0.5
            throttle = 0.5
        elif msg.data == TURN_LEFT:
            steer = -0.5
            throttle = 0.5

        control_msg = ControlMessage(
            steer, throttle, brake, hand_brake, reverse, msg.timestamp)
        self.get_output_stream('action_stream').send(control_msg)

    def execute(self):
        self.spin()
