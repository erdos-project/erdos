ERDOS
=================================

*ERDOS is a dataflow execution engine designed for building autonomous vehicle and robotics applications.*


View the `codebase on GitHub`_.

.. TODO: update the codebase URL
.. _`codebase on GitHub`: https://github.com/erdos-project/erdos


Example Use
-----------

.. code-block:: python

  graph = erdos.graph.get_current_graph()

  # Construct vertices of the graph. Vertices correspond to operators which
  # receive, send, and transform data.
  # Create a camera operator which generates a stream of RGB images
  camera_op = graph.add(CameraOp)
  # Create an object detection operator with the provided model
  object_det_op = graph.add(ObjectDetectorOp,
                            args=["models/ssd_mobilenet_v1_coco"])
  # Create semantic segmentation operator with the provided model
  segmentation_op = graph.add(SegmentationOp,
                              args=["models/drn_d_22_cityscapes"])
  # Create an action operator to propose actions from provided features
  action_op = graph.add(ActionOp)
  # Create a robot operator to interface with the robot
  robot_op = graph.add(RobotOp)

  # Construct edges of the graph by connecting operators
  graph.connect([camera_op], [object_det_op, segmentation_op])
  graph.connect([object_det_op, segmentation_op], [action_op])
  graph.connect([action_op], [robot_op])

  # Execute the application
  graph.execute()


.. toctree::
    :maxdepth: 1
    :caption: Getting Started

    graphs.rst
    operators.rst
    data_streams.rst
    messages_and_timestamps.rst
    interfacing_with_ros.rst
