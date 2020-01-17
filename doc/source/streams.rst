Streams
=======

Streams are used to send messages in ERDOS applications.

Operators send and read messages from streams to communicate with other
operators and ingest/extract data from the system.

ERDOS streams are similar to ROS topics, but have few additional desirable
properties. Streams facilitate one-to-many communication, so only 1 operator
sends messages on a stream. ERDOS broadcasts messages sent on a stream to all
connected operators. In addition, streams are typed when using the Rust API.

Use the following interfaces to send and receive data from streams:
Read Stream, Write Stream, Ingest Stream, and Extract Stream.

Sending Messages
----------------

Operators use Write Streams to send data.

.. autoclass:: erdos.WriteStream
    :members: send


Receiving Messages
------------------

Operators receive data by registering callbacks or manually reading messages
from Read Streams.

Callbacks are functions which take an ERDOS message and any necessary write
streams as arguments. Generally, callbacks process received messages and
publish the results on write streams.

.. autoclass:: erdos.ReadStream
    :members: read, try_read, add_callback, add_watermark_callback


Ingesting and Extracting Data
-----------------------------

Some applications have trouble placing all of the data processing logic inside
operators. For these applications, ERDOS provides special stream interfaces to
*ingest* and *extract* data.

A comprehensive example is available `here <https://github.com/erdos-project/erdos/blob/master/python/examples/ingest_extract.py>`_.

.. autoclass:: erdos.IngestStream
    :members: send

.. autoclass:: erdos.ExtractStream
    :members: read, try_read


Loops
-----
Certain applications require feedback in the dataflow. To support this use
case, ERDOS provides the LoopStream interface to support loops in the
dataflow.

A comprehensive example is available `here <https://github.com/erdos-project/erdos/blob/master/python/examples/loop.py>`_.

.. autoclass:: erdos.LoopStream
    :members: set
