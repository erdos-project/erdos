import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from utils import add_bounding_box


class TrackerCV2Operator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(TrackerCV2Operator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._output_stream_name = output_stream_name
        self._bridge = CvBridge()
        self._tracker = cv2.TrackerKCF_create()
        self._to_process = []
        self._initialized = False
        self._last_seq_num = -1

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        def is_obstacles_stream(stream):
            return stream.labels.get('obstacles', '') == 'true'

        def is_camera_stream(stream):
            return stream.labels.get('camera', '') == 'true'

        input_streams.filter(is_obstacles_stream).add_callback(
            TrackerCV2Operator.on_objects_msg)
        input_streams.filter(is_camera_stream).add_callback(
            TrackerCV2Operator.on_frame_msg)
        return [DataStream(name=output_stream_name)]

    def on_frame_msg(self, msg):
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        start_time = time.time()
        image_np = self._bridge.imgmsg_to_cv2(msg.data, 'rgb8')
        self._to_process.append(image_np)
        if self._initialized:
            for frame in self._to_process:
                ok, bbox = self._tracker.update(frame)
                if not ok:
                    self._logger.error('Tracker failed')
                else:
                    (xmin, ymin, w, h) = bbox
                    corners = (xmin, xmin + w, ymin, ymin + h)

                    if self._flags.visualize_tracker_output:
                        img = Image.fromarray(np.uint8(frame)).convert('RGB')
                        add_bounding_box(img, corners)
                        # cv2.rectangle(frame,  (xmin, ymin), (xmin + w, ymin + h),
                        #              (255, 0, 0), 2, 1)
                        draw = ImageDraw.Draw(img)
                        draw.text((5, 5),
                                  "Timestamp: {}".format(msg.timestamp),
                                  fill='black')
                        open_cv_image = np.array(img)
                        open_cv_image = open_cv_image[:, :, ::-1].copy()
                        cv2.imshow(self.name, open_cv_image)
                        cv2.waitKey(1)

                    output_msg = Message([corners], msg.timestamp)
                    self.get_output_stream(
                        self._output_stream_name).send(output_msg)

            self._to_process = []

        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},"{}",{}'.format(
            time_epoch_ms(), self.name, msg.timestamp, runtime))

    def on_objects_msg(self, msg):
        # TODO(ionel): Implement out bbox matching!
        # TODO(ionel): Frames and bbox are not always corectly associated.
        if len(msg.data) > 0:
            (corners, score, obstacle_class) = msg.data[0]
            (xmin, xmax, ymin, ymax) = corners
            if not self._initialized and len(self._to_process) > 0:
                self._initialized = True
                frame = self._to_process.pop(0)
                tbbox = (xmin, ymin, xmax - xmin, ymax - ymin)
                ok = self._tracker.init(frame, tbbox)
                if not ok:
                    self._logger.error('Tracker init failed')

    def execute(self):
        self.spin()
