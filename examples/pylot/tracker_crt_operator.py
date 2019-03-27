import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image
import time

from dependencies.conv_reg_vot.simgeo import Rect
import dependencies.conv_reg_vot.tracker as tracker

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

from utils import add_bounding_box


class TrackerCRTOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None):
        super(TrackerCRTOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._output_stream_name = output_stream_name
        self.init_image = None
        self.init_bb = None
        self.initialized = False
        self.tracker = None
        self._bridge = CvBridge()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        def is_obstacles_stream(stream):
            return stream.labels.get('obstacles', '') == 'true'

        def is_camera_stream(stream):
            return stream.labels.get('camera', '') == 'true'

        input_streams.filter(is_obstacles_stream).add_callback(
            TrackerCRTOperator.on_objects_msg)
        input_streams.filter(is_camera_stream).add_callback(
            TrackerCRTOperator.on_frame_msg)
        return [DataStream(name=output_stream_name)]

    def on_frame_msg(self, msg):
        start_time = time.time()
        if not self.initialized:
            self.init_image = self._bridge.imgmsg_to_cv2(msg.data, 'rgb8')
        else:
            current_image = self._bridge.imgmsg_to_cv2(msg.data, 'rgb8')
            bb_pred = self.tracker.track(current_image)
            xmax = bb_pred.x + bb_pred.w
            ymax = bb_pred.y + bb_pred.h
            corners = (bb_pred.x, xmax, bb_pred.y, ymax)

            if self._flags.visualize_tracker_output:
                img = Image.fromarray(np.uint8(current_image)).convert('RGB')
                add_bounding_box(img, corners)
                # cv2.rectangle(current_image, (bb_pred.x, bb_pred.y), (xmax, ymax),
                #               (255, 0, 0), 2, 1)
                open_cv_image = np.array(img)
                open_cv_image = open_cv_image[:, :, ::-1].copy()
                cv2.imshow(self.name, open_cv_image)
                cv2.waitKey(1)

            runtime = time.time() - start_time
            self._logger.info('Object tracker crt {} runtime {}'.format(
                self.name, runtime))

            output_msg = Message([corners], msg.timestamp)
            self.get_output_stream(self._output_stream_name).send(output_msg)

    def on_objects_msg(self, msg):
        if len(msg.data) > 0:
            (corners, score, obstacle_class) = msg.data[0]
            (xmin, xmax, ymin, ymax) = corners
            rect_coordinates = (int(xmin), int(ymin),
                                int(xmax - xmin), int(ymax - ymin))
            if not self.initialized and self.init_image is not None:
                self.tracker = tracker.ConvRegTracker()
                self.tracker.init(self.init_image, Rect(*rect_coordinates))
                self.initialized = True

    def execute(self):
        self.spin()
