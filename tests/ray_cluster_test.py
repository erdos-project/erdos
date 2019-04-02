from __future__ import print_function

from absl import app
from absl import flags

from ray.tests.cluster_utils import Cluster

import ray.worker

import erdos.graph
from erdos.op import Op
from erdos.utils import frequency

FLAGS = flags.FLAGS

# Maps plasma store socket name -> node id
node_directory = {}


class MachineAOp(Op):
    def __init__(self, name):
        super(MachineAOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        return []

    @frequency(1)
    def print_node(self):
        plasma_store_socket_name = (
            ray.worker.global_worker.plasma_client.store_socket_name)
        node_name = node_directory.get(plasma_store_socket_name, "unknown")
        print("MachineAOp is located on node {}".format(node_name))

    def execute(self):
        self.print_node()
        self.spin()


class MachineBOp(Op):
    def __init__(self, name):
        super(MachineBOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        return []

    @frequency(1)
    def print_node(self):
        plasma_store_socket_name = (
            ray.worker.global_worker.plasma_client.store_socket_name)
        node_name = node_directory.get(plasma_store_socket_name, "unknown")
        print("MachineBOp is located on node {}".format(node_name))

    def execute(self):
        self.print_node()
        self.spin()


def main(argv):
    # Set up graph
    graph = erdos.graph.get_current_graph()

    # Add operators
    machine_a_op = graph.add(MachineAOp, name='machine_a_op',
                            resources={"machine_a": 1})
    machine_b_op = graph.add(MachineBOp, name='machine_b_op',
                            resources={"machine_b": 1})

    # Execute graph
    graph.execute("ray")


if __name__ == "__main__":
    cluster = Cluster(initialize_head=True)
    a = cluster.add_node(resources={"machine_a": 100}, num_cpus=4)
    b = cluster.add_node(resources={"machine_b": 100}, num_cpus=4)

    node_directory[a.plasma_store_socket_name] = "machine_a"
    node_directory[b.plasma_store_socket_name] = "machine_b"

    print("node a plasma_store_socket_name:\n\t{}".format(
        a.plasma_store_socket_name))
    print("node b plasma_store_socket_name:\n\t{}".format(
        b.plasma_store_socket_name))

    FLAGS.ray_redis_address = cluster.redis_address
    app.run(main)
