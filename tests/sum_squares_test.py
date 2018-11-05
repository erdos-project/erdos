from __future__ import print_function

from absl import app
from absl import flags
import numpy as np

try:
    from std_msgs.msg import Int64
except ModuleNotFoundError:
    # ROS not installed
    Int64 = int

from erdos.data_stream import DataStream
from erdos.message import Message
import erdos.graph
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


class IntegerOp(Op):
    """Operator which publishes an integer every second"""

    def __init__(self, name, number):
        super(IntegerOp, self).__init__(name)
        self.number = np.int64(number)

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=Int64, name="integer_out")]

    @frequency(1)
    def publish_random_number(self):
        output_msg = Message(self.number, Timestamp(coordinates=[0]))
        self.get_output_stream("integer_out").send(output_msg)
        print("%s sent %d" % (self.name, self.number))

    def execute(self):
        self.publish_random_number()
        self.spin()


class SquareOp(Op):
    """Operator which publishes the square of its input"""

    def __init__(self, name):
        super(SquareOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SquareOp.on_next)
        return [DataStream(data_type=Int64, name="square_output")]

    def on_next(self, msg):
        value = msg.data
        result = value**2
        self.get_output_stream("square_output").send(
            Message(result, msg.timestamp))
        print("%s received: %d ^ 2 = %d" % (self.name, value, result))

    def execute(self):
        self.spin()


class SumOp(Op):
    """Operator which sums the most recently published values for each input.

    Sum operation occurs once every second.
    """

    def __init__(self, name):
        super(SumOp, self).__init__(name)
        self.sum = 0

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SumOp.add)
        return [DataStream(data_type=Int64, name="sum_output")]

    @frequency(1)
    def publish_sum(self):
        result = self.sum
        output_msg = Message(result, Timestamp(coordinates=[0]))
        self.get_output_stream("sum_output").send(output_msg)

    def add(self, msg):
        value = msg.data
        original = self.sum
        self.sum += msg.data
        print("%s: %d (original) + %d (received) = %d (result)"
              % (self.name, original, value, self.sum))

    def execute(self):
        self.publish_sum()
        self.spin()


def main(argv):
    """Sums the squares of 2 numbers. """

    # Set up graph
    graph = erdos.graph.get_current_graph()

    # Add operators
    int1 = graph.add(IntegerOp, name='int1', init_args={'number': 1})
    int2 = graph.add(IntegerOp, name='int2', init_args={'number': 2})
    square1 = graph.add(SquareOp, name='square')
    square2 = graph.add(SquareOp, name='square2')
    sum = graph.add(SumOp, name='sum')

    # Connect operators
    graph.connect([int1], [square1])
    graph.connect([int2], [square2])
    graph.connect([square1, square2], [sum])

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == "__main__":
    app.run(main)
