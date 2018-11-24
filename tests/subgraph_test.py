from __future__ import print_function

import time
from absl import app
from absl import flags
from multiprocessing import Process
import numpy as np

try:
    from std_msgs.msg import Int64
except ModuleNotFoundError:
    # ROS not installed
    Int64 = int

from erdos.data_stream import DataStream
import erdos.graph
from erdos.graph import Graph
from erdos.op import Op
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency

from sum_squares_test import SquareOp, SumOp

FLAGS = flags.FLAGS


class IntegerOp(Op):
    """Operator which publishes an integer every second"""

    def __init__(self, name, number, stream_name):
        super(IntegerOp, self).__init__(name)
        self.number = np.int64(number)
        self.stream_name = stream_name

    @staticmethod
    def setup_streams(input_streams, stream_name="integer_out"):
        return [DataStream(data_type=Int64, name=stream_name)]

    @frequency(1)
    def publish_random_number(self):
        output_msg = Message(self.number, Timestamp(coordinates=[0]))
        self.get_output_stream(self.stream_name).send(output_msg)
        print("%s sent %d" % (self.name, self.number))

    def execute(self):
        self.publish_random_number()
        self.spin()


class SumSquaresGraph(Graph):
    def construct(self, input_ops):
        square_op = self.add(SquareOp, "square")
        sum_op = self.add(SumOp, name="sum")
        self.connect(input_ops, [square_op])
        self.connect([square_op], [sum_op])
        return [sum_op]


def run_graph(argv):
    """Sums the squares of 2 numbers. """

    graph = erdos.graph.get_current_graph()
    sub_graph = graph.add(SumSquaresGraph, name="sum_squares")

    # Add operators
    int1 = graph.add(
        IntegerOp,
        name="int1",
        init_args={
            "number": 1,
            "stream_name": "int1_out"
        },
        setup_args={"stream_name": "int1_out"})
    int2 = graph.add(
        IntegerOp,
        name="int2",
        init_args={
            "number": 2,
            "stream_name": "int2_out"
        },
        setup_args={"stream_name": "int2_out"})
    square_op = graph.add(SquareOp, name="default_square")

    # Connect operators
    graph.connect([int1, int2], [sub_graph])
    graph.connect([sub_graph], [square_op])

    # Execute graph
    graph.execute(FLAGS.framework)


def main():
    proc = Process(target=run_graph)
    proc.start()
    time.sleep(10)
    proc.terminate()


if __name__ == "__main__":
    app.run(main)
