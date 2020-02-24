ERDOS
=====

*ERDOS is a platform for developing self-driving cars and robotics
applications.*


View the `codebase on GitHub`_.

.. TODO: update the codebase URL
.. _`codebase on GitHub`: https://github.com/erdos-project/erdos


Example Use
-----------

.. code-block:: python

  # Construct the dataflow graph in the driver.
  # The dataflow graph consists of operators which process data,
  # and streams which broadcast messages to other operators.
  # Create a camera operator which generates a stream of RGB images.
  camera_stream = erdos.connect(CameraOp)
  # Create an object detection operator with the provided model.
  # The object detection operator reads RGB images and sends bounding boxes.
  bounding_box_stream = erdos.connect(ObjectDetectorOp, [camera_stream],
                                      model="models/ssd_mobilenet_v1_coco")
  # Create semantic segmentation operator with the provided model
  segmentation_stream = erdos.connect(SegmentationOp, [camera_stream],
                                      model="models/drn_d_22_cityscapes")
  # Create an action operator to propose actions from provided features
  action_op = erdos.connect(ActionOp, [bounding_box_stream, segmentation_stream])
  # Create a robot operator to interface with the robot
  erdos.connect(RobotOp)
  # Execute the application
  erdos.run()


.. toctree::
    :maxdepth: 1
    :caption: Getting Started

    operators.rst
    streams.rst
    messages.rst
