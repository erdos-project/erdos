
from erdos import IngressStream
from erdos.internal import PyGraph

class Graph:
    """An ERDOS dataflow graph representation on which streams and operators
    are added.
    """

    def __init__(self):
        """Constructs a :py:class:`Graph`"""
        self._py_graph = PyGraph()

    def add_ingress(self, name) -> IngressStream:
        self._py_graph.add_ingress(name)
