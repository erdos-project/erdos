Messages
========

ERDOS applications send data on streams via messages. Messages wrap data and
provide timestamp information used to resolve control loops and track data flow
through the system.

Timestamps consist of an array of coordinates. Timestamp semantics are
user-defined for now; however, we may eventually formalize their use in the
future in order to provide more advanced features in order to scale up stateful
operators. Generally, the 0th coordinate is used to track message's sequence
number and subsequent coordinates track the message's progress in cyclic data
flows.

.. autoclass:: erdos.Message

.. autoclass:: erdos.Timestamp
