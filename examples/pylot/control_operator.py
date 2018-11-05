import numpy as np

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

import planner.planner_operator

# Constants Used for the high level commands
REACH_GOAL = 0.0
GO_STRAIGHT = 5.0
TURN_RIGHT = 4.0
TURN_LEFT = 3.0
LANE_FOLLOW = 2.0


class ControlOperator(Op):
    def __init__(self, name):
        super(ControlOperator, self).__init__(name)
        self._logger = setup_logging(self.name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(ControlOperator.compute_action)
        return [DataStream(name='action_stream')]

    def compute_action(self, msg):
        action = None
        if msg.data == REACH_GOAL:
            action = {
                'steer': 0.0,
                'throttle': 0.0,
                'brake': 0.5,
                'hand_brake': False,
                'reverse': False
            }
        elif msg.data == GO_STRAIGHT or msg.data == LANE_FOLLOW:
            action = {
                'steer': 0.0,
                'throttle': 0.5,
                'brake': 0.0,
                'hand_brake': False,
                'reverse': False
            }
        elif msg.data == TURN_RIGHT:
            action = {
                'steer': 0.5,
                'throttle': 0.5,
                'brake': 0.0,
                'hand_brake': False,
                'reverse': False
            }
        elif msg.data == TURN_LEFT:
            action = {
                'steer': -0.5,
                'throttle': 0.5,
                'brake': 0.0,
                'hand_brake': False,
                'reverse': False
            }
        output_msg = Message(action, msg.timestamp)
        self.get_output_stream('action_stream').send(output_msg)

    def execute(self):
        self.spin()
