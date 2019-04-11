import ray
from absl import flags

from erdos.cluster.node import Node
from erdos.ray.ray_node import RayNode, LocalRayNode

FLAGS = flags.FLAGS


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
        self._initialize_ray()

    def _initialize_ray(self):
        ray_nodes = [
            n for n in self.nodes if isinstance(n, (RayNode, LocalRayNode))
        ]
        if len(ray_nodes) == 0:
            return
        local_ray_node = self._find_local_ray_node()

        if local_ray_node is None:
            # Set resources to 0 so no operators run on this node
            if FLAGS.ray_redis_address == "":
                info = ray.init(num_cpus=0, num_gpus=0)
                self.ray_redis_address = info["redis_address"]
            else:
                ray.init(num_cpus=0,
                         num_gpus=0,
                         redis_address=FLAGS.ray_redis_address)
                self.ray_redis_address = FLAGS.ray_redis_address
        else:
            self.ray_redis_address = local_ray_node.setup()
            ray_nodes.remove(local_ray_node)

        for node in ray_nodes:
            node.setup(self.ray_redis_address)

    def _find_local_ray_node(self):
        local_ray_nodes = [
            n for n in self.nodes if isinstance(n, LocalRayNode)
        ]
        if len(local_ray_nodes) == 0:
            return None
        elif len(local_ray_nodes) == 1:
            return local_ray_nodes[0]
        else:
            raise Exception("Multiple local Ray nodes are unsupported")

    def broadcast(self, command):
        for node in self.nodes:
            node.run_command_sync(command)
