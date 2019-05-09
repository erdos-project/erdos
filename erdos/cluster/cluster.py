from absl import flags

from erdos.cluster.node import Node
from erdos.ray.ray_node import LocalRayNode
from erdos.ros.ros_node import ROSNode, LocalROSNode

FLAGS = flags.FLAGS


class Cluster(object):
    """Cluster base class.

    All clusters must inherit from this class, and must implement:

    1. initialize: Initializes the cluster by connecting all nodes.
    """

    def __init__(self):
        self.nodes = []
        self.ray_redis_address = ""
        self.ros_master_uri = ""

    def add_node(self, node):
        if not isinstance(node, Node):
            raise TypeError("'node' must be an instance of 'Node'")

        self.nodes.append(node)

    def initialize(self):
        ros_nodes = {
            n
            for n in self.nodes if isinstance(n, (ROSNode, LocalROSNode))
        }
        ray_nodes = set(self.nodes) - ros_nodes

        head_node = None
        if len(ros_nodes) > 0:
            for n in ros_nodes:
                if isinstance(n, LocalROSNode):
                    head_node = n
                    self.ray_redis_address, self.ros_master_uri = (
                        head_node.setup())
                    break
        else:
            for n in ray_nodes:
                if isinstance(n, LocalRayNode):
                    head_node = n
                    self.ray_redis_address = head_node.setup()
                    break
        if head_node is None:
            raise Exception("Cluster has no head node")

        # Note: Only support 1 local node for now
        for ros_node in ros_nodes:
            if ros_node is not head_node:
                ros_node.setup(self.ray_redis_address, self.ros_master_uri)

        for ray_node in ray_nodes:
            if ray_node is not head_node:
                ray_node.setup(self.ray_redis_address)

    def broadcast(self, command):
        for node in self.nodes:
            node.run_command_sync(command)
