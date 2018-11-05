from cv_bridge import CvBridge
from collections import namedtuple
import numpy as np
from geometry_msgs.msg import Quaternion
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op

LocateObjectType = namedtuple(
    "LocateObjectType", "des_EE_xyz, des_orientation_EE, des_EE_xyz_above")


class MockLocateObjectOperator(Op):
    """
    Mocks the object location algorithm by returning the fixed positions of the
    block.
    """
    stream_name = "mock-locate-object"

    def __init__(self, name, image_stream_name, depth_stream_name,
                 trigger_stream_name):
        """
        Initializes the kinect streams and a cv2 bridge to convert the images
        from ros images to cv2 images.
        """
        super(MockLocateObjectOperator, self).__init__(name)
        self._image_stream_name = image_stream_name
        self._depth_stream_name = depth_stream_name
        self._trigger_stream_name = trigger_stream_name
        self.bridge = CvBridge()
        self.color_image = None
        self.depth_image = None

    @staticmethod
    def setup_streams(input_streams, image_stream_name, depth_stream_name,
                      trigger_stream_name):
        """
        Listens on the image and depth streams, and returns an output stream
        that sends the location of the detected object.
        """
        input_streams.filter_name(image_stream_name)\
            .add_callback(MockLocateObjectOperator.store_color_image)
        input_streams.filter_name(depth_stream_name)\
            .add_callback(MockLocateObjectOperator.store_depth_image)
        input_streams.filter_name(trigger_stream_name)\
            .add_callback(MockLocateObjectOperator.calc_object_loc)
        return [
            DataStream(
                data_type=LocateObjectType,
                name=MockLocateObjectOperator.stream_name)
        ]

    def store_color_image(self, msg):
        """
        Store the RGB image from the Kinect camera.
        """
        try:
            cv_image = self.bridge.imgmsg_to_cv2(msg.data, "bgr8")
        except CvBridgeError as e:
            print(e)
        self.color_image = cv_image

    def store_depth_image(self, msg):
        """
        Store the depth image from the Kinect camera.
        """
        try:
            cv_image = self.bridge.imgmsg_to_cv2(msg.data, "16UC1")
        except CvBridgeError as e:
            print(e)
        self.depth_image = cv_image

    def calc_object_loc(self, msg):
        """
        Given the RGB and the depth image, this function returns the location
        of the detected object.
        """
        des_EE_xyz = np.array([0.45, 0.155, -0.129])
        des_orientation_EE = Quaternion(
            x=-0.00142460053167,
            y=0.999994209902,
            z=-0.00177030764765,
            w=0.00253311793936)
        des_EE_xyz_above = des_EE_xyz + np.array([0, 0, 0.15])
        send_data = LocateObjectType(des_EE_xyz, des_orientation_EE,
                                     des_EE_xyz_above)
        send_msg = Message(send_data, msg.timestamp)
        while True:
            if self.color_image is not None and self.depth_image is not None:
                self.get_output_stream(MockLocateObjectOperator.stream_name).\
                    send(send_msg)
                self.color_image, self.depth_image = None, None
                return
            else:
                time.sleep(0.01)
