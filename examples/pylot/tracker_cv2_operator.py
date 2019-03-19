from absl import flags
import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

from utils import add_bounding_box

FLAGS = flags.FLAGS


class TrackerCV2Operator(Op):
    def __init__(self, name, output_stream_name):
        super(TrackerCV2Operator, self).__init__(name)
        self._logger = setup_logging(self.name, FLAGS.log_file_name)
        self._output_stream_name = output_stream_name
        self._bridge = CvBridge()
        self._tracker = cv2.TrackerKCF_create()
        self._to_process = []
        self._initialized = False

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

                    if FLAGS.visualize_tracker_output:
                        img = Image.fromarray(np.uint8(frame)).convert('RGB')
                        add_bounding_box(img, corners)
                        # cv2.rectangle(frame,  (xmin, ymin), (xmin + w, ymin + h),
                        #              (255, 0, 0), 2, 1)
                        open_cv_image = np.array(img)
                        open_cv_image = open_cv_image[:, :, ::-1].copy()
                        cv2.imshow(self.name, open_cv_image)
                        cv2.waitKey(1)

                    output_msg = Message([corners], msg.timestamp)
                    self.get_output_stream(self._output_stream_name).send(output_msg)

            self._to_process = []
                
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
