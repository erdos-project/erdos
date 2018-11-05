Operators
=========

Operators represent vertices in the execution graph.

All operators must inherit from the `erdos.Op` class.

Operators can receive data by subscribing to data streams provided in the
`setup_streams` method. Operators can publish data on data streams created in
and returned by the `setup_streams` method.
In ERDOS we expect operator developers to specify which output data streams
they write to, and which input streams they subscribe to. For more details,
see the `data streams documentation <data_streams.html>`__.

Operators can set up state by overriding the `__init__` method; however,
operators that do so should call `Op.__init__` as well.

Implement execution logic by overriding the `execute` method. This method may
contain a control loop or call methods that run regularly.


API
---
.. autoclass:: erdos.op.Op
    :members: setup_streams, execute, get_output_stream


Example: Periodically Publishing Data
-------------------------------------
.. literalinclude:: _literalinclude/examples/sum_squares_example.py
    :pyobject: RandomOp


Example: Subscribing to Data Streams
------------------------------------
.. literalinclude:: _literalinclude/examples/sum_squares_example.py
    :pyobject: SquareOp
