Messages
========

ERDOS applications send data on streams via messages. Messages wrap data and
provide timestamp information used to resolve control loops and track data flow
through the system.

.. autoclass:: erdos.Message


Timestamps
----------

Timestamps consist of an array of coordinates. Timestamp semantics are
user-defined for now; however, we may eventually formalize their use in the
future in order to provide more advanced features in order to scale up stateful
operators. Generally, the 0th coordinate is used to track message's sequence
number and subsequent coordinates track the message's progress in cyclic data
flows.

.. autoclass:: erdos.Timestamp


Watermarks
----------

Watermarks in ERDOS signal completion of computation. More concretely,
sending a watermark with timestamp ``t`` on a stream asserts that all future
messages sent on that stream will have timestamps ``t' > t``.
ERDOS also introduces a *top watermark*, which is a watermark with the
maximum possible timestamp. Sending a top watermark closes the stream as
there is no ``t' > t_top``, so no more messages can be sent.

.. autoclass:: erdos.WatermarkMessage
