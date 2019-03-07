import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging


class TrackerCV2Operator(Op):
    def __init__(self, name='motion-planner'):
        super(TrackerCV2Operator, self).__init__(name)
        self._logger = setup_logging(self.name)
        self._bridge = CvBridge()
        self._tracker = cv2.TrackerKCF_create()
        self._to_process = []
        self._init_tracker = False

    @staticmethod
    def setup_streams(input_streams):
        def is_camera_stream(stream):
            return stream.labels.get('camera', '') == 'true'

        input_streams.filter_name('obj_stream').add_callback(
            TrackerCV2Operator.on_objects_msg)
        input_streams.filter(is_camera_stream).add_callback(
            TrackerCV2Operator.on_frame_msg)
        return [DataStream(name='tracker_stream')]

    def on_frame_msg(self, msg):
        image_np = self._bridge.imgmsg_to_cv2(msg.data, "bgr8")
        self._to_process.append(image_np)
        if self._init_tracker:
            for frame in self._to_process:
                ok, bbox = self._tracker.update(frame)
                if not ok:
                    self._logger.error('Tracker failed')
                else:
                    xmin = int(bbox[0])
                    ymin = int(bbox[1])
                    w = int(bbox[2])
                    h = int(bbox[3])
                    cv2.rectangle(frame,  (xmin, ymin), (xmin + w, ymin + h),
                                  (255, 0, 0), 2, 1)
                    #img = Image.fromarray(np.uint8(frame)).convert('RGB')
                    #img.show()
            self._to_process = []
                
    def on_objects_msg(self, msg):
        # TODO(ionel): Implement out bbox matching!
        # TODO(ionel): Frames are bbox are not always corectly associated.
        bbox = msg.data[0]
        if not self._init_tracker and len(self._to_process) > 0:
            self._init_tracker = True
            frame = self._to_process.pop(0)
            xmin = int(bbox[0])
            xmax = int(bbox[1])
            ymin = int(bbox[2])
            ymax = int(bbox[3])
            tbbox = (xmin, ymin, xmax - xmin, ymax - ymin)
            ok = self._tracker.init(frame, tbbox)
            if not ok:
                self._logger.error('Tracker init failed')

    def execute(self):
        self.spin()
