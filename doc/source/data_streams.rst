Data Streams
============

Data streams represent edges in the execution graph.

Operators publish on and subscribe to data streams in order to send and
receive messages.

ERDOS streams are similar to ROS topics, but have few additional desirable
properties. Streams include a set of labels implemented as immutable
(key, value) string pairs. Labels should to specify stream properties that are
meaningful to the application and can be used to select streams that have
certain desirable properties.

In ERDOS, a developer declares a data stream in the following way:

.. code-block:: python

  DataStream(data_type, name, labels)

The developer must specify the type of the messages sent on the data stream,
and a dictionary of labels. The labels are key/value strings:

.. code-block:: python

  labels = {
    "camera_location" : "left",
    "camera_type" : "RGB"
  }

Labels are not necessarily unique and do not guarantee a unique way of
identifying a stream. Instead, streams also have an associated name and a
unique identifier. The name can be specified by the user, and may include a
namespace (e.g., `/robots/robot1`). If the name is not specified, ERDOS
automatically populates this field. In contrast, the unique identifier is
automatically generated.


Naming Data Streams and Consistent Labeling
-------------------------------------------
Because ERDOS does not enforce data stream names and labels, names and labels
may become inconsistent and arbitrary as the complexity of the graph grows.
For this reason, we suggest that every application includes a central document
describing data stream names and valid keys and values for labels. Documenting
operators with the names and labels of expected input streams and
output streams will also improve consistency and debuggability.


Filtering Data Streams
-----------------------
When connecting operators using `graph.connect(op1, op2)`, ERDOS will pass all
output data streams from `op1` to `op2`'s `setup_streams` method using a
`DataStreams` object. ERDOS provides methods to filter the provided data
in case that `op2` intends to subscribe to a subset of `op1`'s output streams.

Filtering may occur by data stream name or labels. For example, the following
would select all camera streams from the input streams:

.. code-block:: python

  camera_streams = input_streams.filter_name("camera")

We might want to further filter camera streams using labels:

.. code-block:: python

  fast_camera_streams = camera_streams.filter(lambda stream: stream.labels.get("FPS") == "60")

As you might notice, data stream names and labels are arbitrary. For suggestions
for ensuring intended behavior when filtering data streams, see the
`naming data streams section <data_streams.html#naming-data-streams-and-consistent-labeling>`__.

.. autoclass:: erdos.data_streams.DataStreams
    :members: filter, filter_name, add_callback


Data Stream API
---------------
Data streams push messages along graph edges from sender to receiver. ERDOS
operators subscribed to these data streams must implement callback methods to
handle messages.

.. autoclass:: erdos.data_stream.DataStream
    :members: send
