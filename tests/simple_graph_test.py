from __future__ import print_function

from absl import app
from absl import flags

import erdos.graph
from erdos.graph import Graph

from sum_squares_test import IntegerOp, SquareOp, SumOp

FLAGS = flags.FLAGS


class SumSquaresGraph(Graph):
    def construct(self, input_ops):
        square_op = self.add(SquareOp, "square")
        sum_op = self.add(SumOp, name="sum")
        self.connect(input_ops, [square_op])
        self.connect([square_op], [sum_op])
        return [sum_op]


def main(argv):
    """Sums the squares of 2 numbers. """

    graph = erdos.graph.get_current_graph()
    sub_graph = graph.add(SumSquaresGraph, name='sum_squares')
    # sub_graph = SumSquaresGraph(name="sum_squares", parent=graph)

    # Add operators
    int1 = graph.add(IntegerOp, name='int1', init_args={'number': 1})
    int2 = graph.add(IntegerOp, name='int2', init_args={'number': 2})
    square_op = graph.add(SquareOp, name='default_square')

    # Connect operators
    graph.connect([int1, int2], [sub_graph])
    graph.connect([sub_graph], [square_op])

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == "__main__":
    app.run(main)
