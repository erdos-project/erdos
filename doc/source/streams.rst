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

Operators receive data by registering callbacks or manually reading messages
from Read Streams.


Ingesting and Extracting Data
-----------------------------

Loops
-----

