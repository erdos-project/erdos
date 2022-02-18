Streams
=======

Streams are used to send data in ERDOS applications.

ERDOS streams are similar to ROS topics, but have a few additional desirable
properties. Streams facilitate one-to-many communication, so only 1 operator
sends messages on a stream.
ERDOS broadcasts messages sent on a stream to all connected operators.
In addition, streams are typed when using the Rust API.

Streams expose 2 classes of interfaces that access the underlying stream:

#. Read-interfaces expose methods to receive and process data. They allow
   pulling data by calling ``read()`` and ``try_read()``.
   Structures that implement read interfaces include:

  * :py:class:`~erdos.ReadStream`: used by operators to read data and register callbacks.

  * :py:class:`~erdos.ExtractStream`: used by the driver to read data.

#. Write-interfaces expose the send method to send data on a stream.
   Structures that implement write interfaces include:

  * :py:class:`~erdos.WriteStream`: used by operators to send data.

  * :py:class:`~erdos.IngestStream`: used by the driver to send data.

Some applications may want to introduce loops in their dataflow graphs which
is possible using the :py:class:`~erdos.LoopStream`.


Sending Messages
----------------

Operators use Write Streams to send data.

.. autoclass:: erdos.WriteStream
    :members: send


Receiving Messages
------------------

Operators receive data by reading messages from Read Streams. Operators also 
receive data by implementing callbacks that are automatically invoked upon 
the receipt of a message.

.. autoclass:: erdos.ReadStream
    :members: read, try_read


Ingesting and Extracting Data
-----------------------------

Some applications have trouble placing all of the data processing logic inside
operators. For these applications, ERDOS provides special stream interfaces to
*ingest* and *extract* data.

A comprehensive example is available `here <https://github.com/erdos-project/erdos/blob/master/python/examples/ingest_extract.py>`__.

.. autoclass:: erdos.IngestStream
    :members: send

.. autoclass:: erdos.ExtractStream
    :members: read, try_read


Loops
-----
Certain applications require feedback in the dataflow. To support this use
case, ERDOS provides the LoopStream interface to support loops in the
dataflow.

A comprehensive example is available `here <https://github.com/erdos-project/erdos/blob/master/python/examples/loop.py>`__.

.. autoclass:: erdos.LoopStream
    :members: connect_loop
