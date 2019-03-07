import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image

#from dependencies.conv_reg_vot.simgeo import Rect
#import dependencies.conv_reg_vot.tracker as tracker

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op


class TrackerCRTOperator(Op):
    def __init__(self, name):
        super(TrackerCRTOperator, self).__init__(name)
        self.init_image = None
        self.init_bb = None
        self.initialized = False
        self.tracker = None
        self._bridge = CvBridge()

    @staticmethod
    def setup_streams(input_streams):
        def is_camera_stream(stream):
            return stream.labels.get('camera', '') == 'true'

        input_streams.filter_name('obj_stream').add_callback(
            TrackerCRTOperator.on_objects_msg)
        input_streams.filter(is_camera_stream).add_callback(
            TrackerCRTOperator.on_frame_msg)
        return [DataStream(name='tracker_stream')]

    def on_frame_msg(self, msg):
        # if not self.initialized:
        #     self.init_image = self._bridge.imgmsg_to_cv2(msg.data, "bgr8")
        # else:
        #     current_image = self._bridge.imgmsg_to_cv2(msg.data, "bgr8")
        #     bb_pred = self.tracker.track(current_image)
        #     xmax = bb_pred.x + bb_pred.w
        #     ymax = bb_pred.y + bb_pred.h
        #     corners = (bb_pred.x, xmax, bb_pred.y, ymax)
        #     cv2.rectangle(current_image, (bb_pred.x, bb_pred.y), (xmax, ymax),
        #                   (255, 0, 0), 2, 1)
        #     img = Image.fromarray(np.uint8(current_image)).convert('RGB')
        #     img.show()

        #     output_msg = Message([corners], msg.timestamp)
        #     self.get_output_stream('tracker_stream').send(output_msg)
        pass

    def on_objects_msg(self, msg):
        (xmin, xmax, ymin, ymax) = msg.data[0]
        rect_coordinates = (int(xmin), int(ymin),
                            int(xmax - xmin), int(ymax - ymin))
        # if not self.initialized and self.init_image is not None:
        #     self.tracker = tracker.ConvRegTracker()
        #     self.tracker.init(self.init_image, Rect(*rect_coordinates))
        #     self.initialized = True

    def execute(self):
        self.spin()
