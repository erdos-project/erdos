import cv2
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import time

from cv_bridge import CvBridge
from collections import deque

from carla.image_converter import depth_to_array

from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from detection_utils import get_avg_precision_at_iou, get_2d_bbox_from_3d_box, get_camera_intrinsic_and_transform


class DetectionEvalGroundOperator(Op):
    def __init__(self,
                 name,
                 rgb_camera_setup,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(DetectionEvalGroundOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._flags = flags
        self._bridge = CvBridge()
        # Queue of incoming data.
        self._rgb_imgs = deque()
        self._world_transforms = deque()
        self._depth_imgs = deque()
        self._pedestrians = deque()
        self._ground_bboxes = deque()
        (camera_name, pp, img_size, pos) = rgb_camera_setup
        (self._rgb_intrinsic, self._rgb_transform,
         self._rgb_img_size) = get_camera_intrinsic_and_transform(
             name=camera_name,
             postprocessing=pp,
             image_size=img_size,
             position=pos)
        self._last_notification = -1
        self._iou_thresholds = [0.1 * i for i in range(1, 10)]

    @staticmethod
    def setup_streams(input_streams, depth_camera_name):
        def is_camera_stream(stream):
            return (stream.labels.get('camera', '') == 'true'
                    and stream.labels.get('ros', '') == 'true')

        input_streams.filter_name(depth_camera_name).add_callback(
            DetectionEvalGroundOperator.on_depth_camera_update)
        input_streams.filter(is_camera_stream).add_callback(
            DetectionEvalGroundOperator.on_rgb_camera_update)
        input_streams.filter_name('world_transform').add_callback(
            DetectionEvalGroundOperator.on_world_transform_update)
        input_streams.filter_name('pedestrians').add_callback(
            DetectionEvalGroundOperator.on_pedestrians_update)

        input_streams.add_completion_callback(
            DetectionEvalGroundOperator.on_notification)
        return []

    def on_notification(self, msg):
        # Check that we didn't skip any notification. We only skip
        # notifications if messages or watermarks are lost.
        if self._last_notification != -1:
            assert self._last_notification + 1 == msg.timestamp.coordinates[1]
        self._last_notification = msg.timestamp.coordinates[1]

        # Ignore the first several seconds of the simulation because the car is
        # not moving at the beginning.
        if msg.timestamp.coordinates[
                0] <= self._flags.eval_ground_truth_ignore_first:
            return

        # Get the data from the start of all the queues and make sure that
        # we did not miss any data.
        depth_msg = self._depth_imgs.popleft()
        rgb_msg = self._rgb_imgs.popleft()
        world_trans_msg = self._world_transforms.popleft()
        pedestrians_msg = self._pedestrians.popleft()

        self._logger.info('Timestamps {} {} {} {}'.format(
            depth_msg.timestamp, rgb_msg.timestamp, world_trans_msg.timestamp,
            pedestrians_msg.timestamp))

        assert (depth_msg.timestamp == rgb_msg.timestamp ==
                world_trans_msg.timestamp == pedestrians_msg.timestamp)

        rgb_img = self._bridge.imgmsg_to_cv2(rgb_msg.data, 'rgb8')
        rgb_img = Image.fromarray(np.uint8(rgb_img)).convert('RGB')
        depth_array = depth_to_array(depth_msg.data)
        world_transform = world_trans_msg.data

        bboxes = []
        self._logger.info(
            'Number of pedestrians {}'.format(len(pedestrians_msg.data)))
        for (pedestrian_index, obj_transform, obj_bbox,
             _) in pedestrians_msg.data:
            bbox = get_2d_bbox_from_3d_box(
                rgb_img, depth_array, world_transform, obj_transform, obj_bbox,
                self._rgb_transform, self._rgb_intrinsic, self._rgb_img_size,
                1.5, 3.0)
            if bbox is not None:
                bboxes.append(bbox)

        # Remove the buffered bboxes that are too old.
        while (len(self._ground_bboxes) > 0 and msg.timestamp.coordinates[0] -
               self._ground_bboxes[0][0].coordinates[0] >
               self._flags.eval_ground_truth_max_latency):
            self._ground_bboxes.popleft()

        for (old_timestamp, old_bboxes) in self._ground_bboxes:
            if (len(bboxes) > 0 or len(old_bboxes) > 0):
                latency = msg.timestamp.coordinates[0] - old_timestamp.coordinates[0]
                precisions = []
                for iou in self._iou_thresholds:
                    precisions.append(get_avg_precision_at_iou(
                        bboxes, old_bboxes, iou))
                self._logger.info("Precision {}".format(precisions))
                avg_precision = float(sum(precisions)) / len(precisions)
                self._logger.info(
                    "The latency is {} and the average precision is {}".format(
                        latency, avg_precision))
                self._csv_logger.info('{},{},{},{}'.format(
                    time_epoch_ms(), self.name, latency, avg_precision))

        # Buffer the new bounding boxes.
        self._ground_bboxes.append((msg.timestamp, bboxes))

    def on_depth_camera_update(self, msg):
        if msg.timestamp.coordinates[
                0] > self._flags.eval_ground_truth_ignore_first:
            self._depth_imgs.append(msg)

    def on_rgb_camera_update(self, rgb_msg):
        if rgb_msg.timestamp.coordinates[
                0] > self._flags.eval_ground_truth_ignore_first:
            self._rgb_imgs.append(rgb_msg)

    def on_world_transform_update(self, transform_msg):
        if transform_msg.timestamp.coordinates[
                0] > self._flags.eval_ground_truth_ignore_first:
            self._world_transforms.append(transform_msg)

    def on_pedestrians_update(self, msg):
        if msg.timestamp.coordinates[
                0] > self._flags.eval_ground_truth_ignore_first:
            self._pedestrians.append(msg)
