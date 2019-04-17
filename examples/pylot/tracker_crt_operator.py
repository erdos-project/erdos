import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import time

from dependencies.conv_reg_vot.simgeo import Rect
import dependencies.conv_reg_vot.tracker as tracker

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from detection_utils import add_bounding_box


class TrackerCRTOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(TrackerCRTOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._output_stream_name = output_stream_name
        self.init_image = None
        self.init_bb = None
        self.initialized = False
        self.tracker = None
        self._bridge = CvBridge()
        self._last_seq_num = -1

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
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

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
                draw = ImageDraw.Draw(img)
                draw.text((5, 5),
                          "Timestamp: {}".format(msg.timestamp),
                          fill='black')

                # cv2.rectangle(current_image, (bb_pred.x, bb_pred.y), (xmax, ymax),
                #               (255, 0, 0), 2, 1)
                open_cv_image = np.array(img)
                open_cv_image = open_cv_image[:, :, ::-1].copy()
                cv2.imshow(self.name, open_cv_image)
                cv2.waitKey(1)

            # Get runtime in ms.
            runtime = (time.time() - start_time) * 1000
            self._csv_logger.info('{},{},"{}",{}'.format(
                time_epoch_ms(), self.name, msg.timestamp, runtime))

            output_msg = Message([corners], msg.timestamp)
            self.get_output_stream(self._output_stream_name).send(output_msg)

    def on_objects_msg(self, msg):
        if len(msg.data) > 0:
            (corners, score, obstacle_class) = msg.data[0]
            (xmin, xmax, ymin, ymax) = corners
            rect_coordinates = (int(xmin), int(ymin),
                                int(xmax - xmin), int(ymax - ymin))
            if not self.initialized and self.init_image is not None:
                # TODO(ionel): Set GPU memory fraction for the tracker.
                self.tracker = tracker.ConvRegTracker()
                self.tracker.init(self.init_image, Rect(*rect_coordinates))
                self.initialized = True

    def execute(self):
        self.spin()
