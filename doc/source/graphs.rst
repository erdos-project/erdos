Graphs
======

ERDOS applications are build as a graph of `operators <operators.html>`__
which are connected by `data streams <data_streams.html>`__.

At a high level, ERDOS developers must specify how operators are  connected
(e.g., an object detector operator is connected to an object tracker), but
they do need to specify the data streams on which connected operators
communicate. This logic is left to operator developers, which must filter the
output streams of the operators they depend on, register callbacks, and define
output streams. Under the hood, ERDOS transforms the high level connections
and operator implementations into a dataflow graph which captures the exact
stream dependencies between operators.


Graph API
---------
.. autoclass:: erdos.graph.Graph
    :members: add, connect, execute


Example Graph
-------------
.. code-block:: python

  graph = erdos.graph.get_current_graph()

  # args are passed to __init__(), and stream_args to setup_streams()
  c1_op = graph.add(CameraOp, args=["left_camera"])
  c2_op = graph.add(CameraOp, args=["right_camera"])
  object_det_op = graph.add(ObjectDetectorOp,
                            args=["models/ssd_mobilenet_v1_coco"],
                            stream_args=["car", "person"])
  tracker_op = graph.add(TrackerOp)

  # c1_op and c2_op provide input streams to object_det_op and tracker_op.
  graph.connect([c1_op, c2_op], [det_op, tracker_op])
  graph.connect([det_op], [tracker_op])
