import cv2
from cv_bridge import CvBridge
import numpy as np
import os

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging

from std_msgs.msg import String
from sensor_msgs.msg import Image

NUM_FRAMES = 50
# TODO(ionel): Getting paths like this is dangerous. Fix!
frames_path = os.path.dirname(os.path.dirname(
    os.path.realpath(__file__))) + '/../pylot/images/'


class CameraOperator(Op):
    def __init__(self, name):
        super(CameraOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._bridge = CvBridge()
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, op_name):
        return [
            DataStream(
                data_type=Image,
                name='{}_output'.format(op_name),
                labels={'camera_type': 'RGB'})
        ]

    @frequency(30)
    def publish_frame(self):
        """Publish frames out camera stream every 100ms."""
        cv_image = self.read_image(self._cnt % NUM_FRAMES)
        image = self._bridge.cv2_to_imgmsg(cv_image, encoding="bgr8")
        image.header.seq = self._cnt
        self._logger.info('%s camera publishing frame %d', self.name,
                          self._cnt)
        output_msg = Message(image, Timestamp(coordinates=[self._cnt]))
        output_name = '{}_output'.format(self.name)
        self.get_output_stream(output_name).send(output_msg)
        self._cnt += 1

    def create_image(self, width, height):
        np_image = np.zeros((width, height, 3), dtype=np.uint8)
        r, g, b = cv2.split(np_image)
        cv_image = cv2.merge([b, g, r])
        return cv_image

    def read_image(self, cnt):
        img = cv2.imread('{}carla{}.jpg'.format(frames_path, cnt))
        return img

    def execute(self):
        """Operator entry method.
        The method implements the operator logic.
        """
        self._logger.info('Executing %s', self.name)
        self.publish_frame()
