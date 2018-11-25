from __future__ import print_function

import pytest

from erdos.graph import Graph
from erdos.operators import NoopOp


def test_invalid_graph_connections():
    parent = Graph(name="parent")

    class Child(Graph):
        def construct(self, input_ops):
            self.connect([parent], [child])
            return []

    child = parent.add(Child, name="child")

    with pytest.raises(Exception):
        parent.connect([child], [parent])

    with pytest.raises(Exception):
        # Should raise an exception in Child.construct
        parent.execute()


def test_invalid_op_connections():
    parent = Graph(name="parent")

    op1 = parent.add(NoopOp)
    other_ops = []

    class Child(Graph):
        def construct(self, input_ops):
            other_ops.append(self.add(NoopOp))
            self.connect([op1], other_ops)
            return [op2]

    child = parent.add(Child, name="child")

    with pytest.raises(Exception):
        parent.connect([op1], [op2])

    with pytest.raises(Exception):
        # Should raise an exception in Child.construct
        parent.execute()
