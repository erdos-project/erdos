import os
import sys
import time
from absl import app
from absl import flags
from multiprocessing import Process

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import erdos.graph
from erdos.graph import Graph

from control_loop_test import FirstOp, SecondOp

FLAGS = flags.FLAGS


class FirstGraph(Graph):
    def construct(self, input_ops):
        first_op = self.add(FirstOp, name='first_op')
        self.connect(input_ops, [first_op])
        return [first_op]


class SecondGraph(Graph):
    def construct(self, input_ops, spin):
        second_op = self.add(
            SecondOp, name='second_op', init_args={'spin': spin})
        self.connect(input_ops, [second_op])
        return [second_op]


def run_graph(spin):
    graph = erdos.graph.get_current_graph()
    first_graph = graph.add(FirstGraph, name='first_graph')
    second_graph = graph.add(
        SecondGraph, name='second_graph', setup_args={'spin': spin})
    graph.connect([first_graph], [second_graph])
    graph.connect([second_graph], [first_graph])
    graph.execute(FLAGS.framework)


def main(argv):
    spin = True
    if FLAGS.framework == 'ray':
        spin = False
    proc = Process(target=run_graph, args=(spin, ))
    proc.start()
    time.sleep(10)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
