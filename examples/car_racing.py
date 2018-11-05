import gym
import numpy as np

import erdos.graph
from erdos.buffered_data_stream import BufferedDataStream
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp


class CarRacingOp(Op):
    def __init__(self, name):
        # Initialize with no input streams
        super(CarRacingOp, self).__init__(name=name)

    @staticmethod
    def setup_streams(input_streams):
        # Add output streams for gym environment
        return [
            DataStream(data_type=float, name="race_reward_output"),
            DataStream(
                data_type=np.ndarray,
                name="race_state_output",
                labels={'state': 'true'}),
            DataStream(data_type=bool, name="race_done_output"),
            DataStream(data_type=dict, name="race_info_output")
        ]

    def on_next(self, action):
        # Update environment
        reward, state, done, info = self.env.step(action)
        self.env.render()  # render on display for user enjoyment

        ts = Timestamp(coordinates=[0])
        # Send data to output streams
        self.get_output_stream("race_reward_output").send(Message(reward, ts))
        self.get_output_stream("race_state_output").send(Message(state, ts))
        self.get_output_stream("race_done_output").send(Message(done, ts))
        self.get_output_stream("race_info_output").send(Message(info, ts))

    def execute(self):
        # Set up environment
        # Must do this here to prevent serialization errors
        self.env = gym.make("CarRacing-v0")
        self.env.reset()

        # Connect to the input stream for actions
        # Assumes action stream is 0th input stream
        # Push implementation
        # self.input_streams[0].register(self.on_next)

        # Trigger sending data
        self.on_next([0, 0, 0])
        # Pull implementation
        while True:
            action = self.input_streams[0].next()
            self.on_next(action)


class ActionOp(Op):
    def __init__(self, name):
        # Attach to race_state_output stream
        super(ActionOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        def is_state_stream(stream):
            return stream.labels.get('state', '') == 'true'

        # Connect to the input stream for state
        # Assumes state stream is 0th input stream
        # Push implementation
        input_streams.filter(is_state_stream)\
            .add_callback(ActionOp.on_next)
        # Add output stream for actions
        return [BufferedDataStream(data_type=list, name="action_output")]

    def on_next(self, state):
        # Send action to output stream
        # action = np.random.random(3)
        action = [0, 1, 0]
        output_msg = Message(action, state.timestamp)
        self.get_output_stream("action_output").send(output_msg)


def main():
    # Create operators
    graph = erdos.graph.get_current_graph()
    car_racing_op = graph.add(CarRacingOp, name='car_racing')
    action_op = graph.add(ActionOp, name='action')

    # Add connect streams to operators for cyclical graph
    graph.connect([car_racing_op], [action_op])
    graph.connect([action_op], [car_racing_op])

    # Execute graph
    graph.execute("ray")


if __name__ == "__main__":
    main()
