from erdos.cluster.node import Node
from erdos.local.local_dispatcher import LocalDispatcher


class LocalNode(Node):
    def __init__(self, resources=None):
        super(LocalNode, self).__init__("127.0.0.1", "", "", resources)

    def _make_dispatcher(self):
        dispatcher = LocalDispatcher()
        self.dispatchers.append(dispatcher)
        return dispatcher
