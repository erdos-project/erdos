import cv2
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import time

from cv_bridge import CvBridge
from collections import deque

from erdos.op import Op
from erdos.utils import setup_logging


class DetectionEvalGroundOperator(Op):
    def __init__(self, name, flags, log_file_name=None, csv_file_name=None):
        super(DetectionEvalGroundOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._flags = flags
        self._bridge = CvBridge()
        # Queue of incoming data.
        self._rgb_imgs = deque()
        self._world_transforms = deque()
        self._last_notification = -1

    @staticmethod
    def setup_streams(input_streams):
        def is_camera_stream(stream):
            return (stream.labels.get('camera', '') == 'true' and
                    stream.labels.get('ros', '') == 'true')

        input_streams.filter(is_camera_stream).add_callback(
            DetectionEvalGroundOperator.on_rgb_camera_update)
        input_streams.filter_name('world_transform').add_callback(
            DetectionEvalGroundOperator.on_world_transform_update)
        input_streams.add_completion_callback(
            DetectionEvalGroundOperator.on_notification)
        return []

    def on_notification(self, msg):
        # Check that we didn't skip any notification. We only skip
        # notifications if messages or watermarks are lost.
        if self._last_notification != -1:
            assert self._last_notification + 1 == msg.timestamp.coordinates[1]
        self._last_notification = msg.timestamp.coordinates[1]

        # Get the data from the start of all the queues and make sure that
        # we did not miss any data.
        (rgb_t, rgb_img) = self._rgb_imgs.popleft()
        (transform_t, world_transform) = self._world_transforms.popleft()

        self._logger.info('Timestamps {} {} {}'.format(
            msg.timestamp, rgb_t, transform_t))
        assert rgb_t == msg.timestamp == transform_t

    def on_rgb_camera_update(self, rgb_msg):
        self._logger.info('Received a camera update at {}'.format(
            rgb_msg.timestamp))
        rgb_img = self._bridge.imgmsg_to_cv2(rgb_msg.data, 'rgb8')
        rgb_img = Image.fromarray(np.uint8(rgb_img)).convert('RGB')
        self._rgb_imgs.append((rgb_msg.timestamp, rgb_img))

    def on_world_transform_update(self, transform_msg):
        self._logger.info('Received a world transform at {}'.format(
            transform_msg.timestamp))
        self._world_transforms.append((transform_msg.timestamp,
                                       transform_msg.data))
