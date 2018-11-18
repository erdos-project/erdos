import cv2
from cv_bridge import CvBridge
import numpy as np
import os

from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging

from sensor_msgs.msg import Image

NUM_FRAMES = 50
# TODO(ionel): Getting paths like this is dangerous. Fix!
frames_path = os.path.dirname(os.path.dirname(
    os.path.realpath(__file__))) + '/../pylot/images/'


class DepthCameraOperator(LoggingOp):
    def __init__(self, name, buffer_logs=False):
        super(DepthCameraOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._bridge = CvBridge()
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, op_name):
        return [
            DataStream(
                data_type=Image,
                name='{}_output'.format(op_name),
                labels={'camera_type': 'depth'})
        ]

    def create_image(self, width, height):
        np_image = np.zeros((width, height, 3), dtype=np.uint8)
        r, g, b = cv2.split(np_image)
        cv_image = cv2.merge([b, g, r])
        return cv_image

    @frequency(30)
    def publish_frame(self):
        # TODO(ionel): Should send RGBD instead of RGB.
        cv_image = self.create_image(1280, 960)
        image = self._bridge.cv2_to_imgmsg(cv_image, encoding="bgr8")
        image.header.seq = self._cnt
        output_msg = Message(image, Timestamp(coordinates=[self._cnt]))
        output_name = '{}_output'.format(self.name)
        self.get_output_stream(output_name).send(output_msg)
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.publish_frame()
        self.spin()
