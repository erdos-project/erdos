from absl import app
from absl import flags

from erdos.graph import Graph
from erdos.cluster.cluster import Cluster
from erdos.ray.ray_node import RayNode, LocalRayNode

from sum_squares_test import IntegerOp, SquareOp, SumOp

FLAGS = flags.FLAGS
flags.DEFINE_string('node_address', '', 'Address of the remote node to use.')
flags.DEFINE_string('username', '',
                    'Username used to log into the remote node.')
flags.DEFINE_string('ssh_key', '', 'SSH key used to log into the remote node')


def main(argv):
    """Sums the squares of 2 numbers on a Ray cluster"""
    # Set up cluster
    cluster = Cluster()

    # Add nodes
    local_node = LocalRayNode()
    remote_node = RayNode(FLAGS.node_address, FLAGS.username, FLAGS.ssh_key)

    cluster.add_node(local_node)
    cluster.add_node(remote_node)

    # Set up graph
    graph = Graph(_cluster=cluster)

    # Add operators
    int1 = graph.add(IntegerOp,
                     name='int1',
                     init_args={'number': 1},
                     _node=local_node)
    int2 = graph.add(IntegerOp,
                     name='int2',
                     init_args={'number': 2},
                     _node=remote_node)
    square1 = graph.add(SquareOp, name='square', _node=local_node)
    square2 = graph.add(SquareOp, name='square2', _node=remote_node)
    sum = graph.add(SumOp, name='sum', _node=remote_node)

    # Connect operators
    graph.connect([int1], [square1])
    graph.connect([int2], [square2])
    graph.connect([square1, square2], [sum])

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == "__main__":
    app.run(main)
