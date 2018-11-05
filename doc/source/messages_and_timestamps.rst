Messages and Timestamps
=======================

ERDOS applications send data on data streams via messages. Messages wrap data
and provide timestamp information used to resolve control loops and track
data flow through the system.

Timestamps consist of an array of coordinates. Timestamp semantics are
user-defined for now; however, we may eventually formalize their use in the
future in order to provide more advanced features in order to scale up
stateful operators. Generally, the 0th coordinate is used to track message's
sequence number and subsequent coordinates track the message's progress in
cyclic data flows.

The following operator method constructs a message and publishes it on a data
stream:

.. literalinclude:: _literalinclude/examples/sum_squares_example.py
    :pyobject: RandomOp.publish_random_number
    :emphasize-lines: 5
    :dedent: 4


Message API
-----------
.. autoclass:: erdos.message.Message
    :members:


Timestamp API
-------------
.. autoclass:: erdos.timestamp.Timestamp
    :members:
