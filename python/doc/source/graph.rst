Graph
========

A user defined dataflow graph representation.

Example
-------

The following example demonstrates how to create a :py:class:`erdos.Graph` and run it.

.. code-block:: python

    # Create the graph.
    graph = Graph()

    # Add streams and operators here.
    ingress_stream = graph.add_ingress("ExampleIngressStream")

    # Run the graph defined above.
    graph.run_async()
    
    # Send data on the ingress stream.
    msg = Message(Timestamp(coordinates=[0]), 10)
    ingress_stream.send(msg)

.. autoclass:: erdos.Graph
    :members:

.. autoclass:: erdos.graph.NodeHandle
    :members:
