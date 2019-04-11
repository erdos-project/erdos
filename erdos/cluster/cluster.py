import ray

from erdos.cluster.node import Node
from erdos.ray.ray_node import RayNode


class Cluster(object):
    """Cluster base class.

    All clusters must inherit from this class, and must implement:

    1. initialize: Initializes the cluster by connecting all nodes.
    """

    def __init__(self):
        self.nodes = []
        self.ray_redis_address = ""

    def add_node(self, node):
        if not isinstance(node, Node):
            raise TypeError("'node' must be an instance of 'Node'")

        self.nodes.append(node)

    def initialize(self):
        info = ray.init() # TODO: set resources for current node
        self.ray_redis_address = info["redis_address"]

        for node in self.nodes:
            if isinstance(node, RayNode):
                node.setup(self.ray_redis_address)
            else:
                raise NotImplementedError(
                    "Cannot setup other types of nodes yet")

        if self.ray_redis_address and not ray.is_initialized():
            ray.init(redis_address=self.ray_redis_address)

    def broadcast(self, command):
        for node in self.nodes:
            node.run_command_sync(command)
