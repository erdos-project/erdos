from erdos.cluster.node import Node


class Cluster(object):
    """Cluster base class.

    All clusters must inherit from this class, and must implement:

    1. initialize: Initializes the cluster by connecting all nodes.
    2. execute: Runs an operator on a node.
    """

    def __init__(self):
        self.nodes = []
    
    def add_node(self, node):
        if not isinstance(node, Node):
            raise TypeError("'node' must be an instance of 'Node'")

        self.nodes.append(node)

    def initialize(self):
        raise NotImplementedError("Must implement 'initialize'")

    def execute(self, node, op):
        raise NotImplementedError("Must implement 'execute'")

    def broadcast(self, command):
        for node in self.nodes:
            node.run_command_sync(command)
