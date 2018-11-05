Interfacing with ROS
====================

Due to the popularity of ROS, ERDOS provides support to connect to existing
ROS applications.

Specifically, ERDOS provides operators to publish on and subscribe to ROS
topics, communicate with ROS services, and interface with ROS actionlib.


ROS Publisher API
-----------------

.. autoclass:: erdos.ros.ros_publisher_op.ROSPublisherOp
    :members: setup_streams


ROS Subscriber API
------------------

.. autoclass:: erdos.ros.ros_subscriber_op.ROSSubscriberOp
    :members: setup_streams


ROS Service API
---------------

.. autoclass:: erdos.ros.ros_service_op.ROSServiceOp
    :members: setup_streams


ROS ActionLib API
-----------------

.. autoclass:: erdos.ros.ros_actionlib_op.ROSActionLibOp
    :members:
