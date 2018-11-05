from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import actionlib
import logging
import ray
import rospy

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.timestamp import Timestamp


@ray.remote
class ROSActionLibOp(object):
    """Bridges between ROS actionlib and ERDOS streams.

    Currently WIP.
    """

    def __init__(self, action_name, action_type, command_stream, results_type,
                 feedback_type):
        self.client = actionlib.SimpleActionClient(action_name, action_type)
        self.client.wait_for_server()
        self.result_stream = DataStream(
            data_type=results_type, name=action_name + '-results')
        self.feedback_stream = DataStream(
            data_type=feedback_type, name=action_name + '-feedback')
        self.command_stream = command_stream
        # TODO(ionel): Figure out stream register so that on_next_command gets invoked.
        self.command_stream.register(on_next_command)

    def on_next_command(self, goal):
        self.client.send_goal(goal, self.on_done, self.on_active,
                              self.on_feedback)

    def on_feedback(self, feedback):
        # Called when feedback is provided
        print('Goal feedback')
        self.feedback_stream.send(
            Message(feedback, Timestamp(coordinates=[0])))

    def on_done(self, state, result):
        # Called when an action completes.
        print('Goal completed')
        self.result_stream.send(Message(result, Timestamp(coordinates=[0])))

    def on_active(self):
        # Goal once when the goal becomes active.
        print('Goal is active')
        pass


# self.client.wait_for_result()
# self.client.get_result()
# self.client.get_state()
