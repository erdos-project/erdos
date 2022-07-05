What is ERDOS?
==============

*ERDOS is a platform for developing self-driving cars and robotics
applications.*

The system is built using techniques from streaming dataflow systems which is
reflected by the API.
Applications are modeled as directed :doc:`graphs <graph>`, in which data flows 
through :doc:`streams <streams>` and is processed by :doc:`operators <operators>`.
Because applications often resemble a sequence of connected operators,
an ERDOS application may also be referred to as a *pipeline*.


Example
-------

The following example demonstrates a toy robotics application which uses
semantic segmentation and the bounding boxes of detected objects to control a
robot.
The example consists of the driver part of the program, which is responsible
for connecting operators via streams. For information on building operators, see 
:doc:`operators <operators>`.

.. code-block:: python

  # Create a dataflow graph to which we add operators and streams.
  graph = Graph()
  # Create a camera operator which generates a stream of RGB images.
  camera_frames = graph.connect_source(CameraOp, erdos.OperatorConfig())
  # Connect an object detection operator which uses the provided model to
  # detect objects and compute bounding boxes.
  bounding_boxes = graph.connect_one_in_one_out(
      ObjectDetectorOp,
      erdos.OperatorConfig(),
      camera_frames,
      model="models/ssd_mobilenet_v1_coco")
  # Connect semantic segmentation operator to the camera which computes the
  # semantic segmentation for each image.
  segmentation = graph.connect_one_in_one_out(SegmentationOp,
                                              erdos.OperatorConfig(),
                                              camera_frames,
                                              model="models/drn_d_22_cityscapes")
  # Connect an action operator to propose actions from provided features.
  actions = erdos.connect_two_in_one_out(ActionOp, erdos.OperatorConfig(),
                                         bounding_boxes, segmentation)
  # Create a robot operator which interfaces with the robot to apply actions.
  graph.connect_sink(RobotOp, erdos.OperatorConfig(), actions)

  # Execute the application.
  graph.run()

Further examples are available on
`GitHub <https://github.com/erdos-project/erdos/tree/master/python/examples>`_


Driver
------

The driver section of the program connects operators together using streams to
build an ERDOS application which may then be executed.
The driver is typically the main section of the program.

The driver may also interact with a running ERDOS application.
Using the :py:class:`.IngressStream`, the driver can send
data to operators on a stream.
The :py:class:`.EgressStream` allows the driver to read
data sent from an operator.


Determinism
-----------

ERDOS provides mechanisms to enable the building of deterministic
applications.
For instance, processing sets of messages separated by watermarks using 
watermark callbacks can turn ERDOS pipelines into
`Kahn process networks <https://en.wikipedia.org/wiki/Kahn_process_networks>`_.


Performance
-----------

ERDOS is designed for low latency. Self-driving car pipelines require
end-to-end deadlines on the order of hundreds of milliseconds for safe
driving. Similarly, self-driving cars typically process gigabytes per
second of data on small clusters. Therefore, ERDOS is optimized to
send small amounts of data (gigabytes as opposed to terabytes)
as quickly as possible.

For performance-sensitive applications, it is recommended to use the Rust API
as Python introduces significant overheads (e.g. serialization and
reduced parallelism from the
`GIL <https://wiki.python.org/moin/GlobalInterpreterLock>`_).

View the `codebase on GitHub <https://github.com/erdos-project/erdos>`_.

You can export the dataflow graph as a 
`DOT file <https://en.wikipedia.org/wiki/DOT_(graph_description_language)>`_
by setting the ``graph_filename`` argument in :py:meth:`erdos.Graph.run`.


More Information
----------------

To read more about the ideas behind ERDOS, refer to our paper:
`*D3: A Dynamic Deadline-Driven Approach for Building Autonomous Vehicles* <https://dl.acm.org/doi/10.1145/3492321.3519576>`_.
If you find ERDOS useful to your work, please consider citing our paper:

.. code-block:: bibtex

  @inproceedings{gog2022d3,
    title={D3: a dynamic deadline-driven approach for building autonomous vehicles},
    author={Gog, Ionel and Kalra, Sukrit and Schafhalter, Peter and Gonzalez, Joseph E and Stoica, Ion},
    booktitle={Proceedings of the Seventeenth European Conference on Computer Systems},
    pages={453--471},
    year={2022}
  }
