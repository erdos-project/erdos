import cv2
import numpy as np
import PIL.Image as Image

from carla.image_converter import labels_to_cityscapes_palette

from erdos.op import Op
from erdos.utils import setup_logging


class SegmentedVideoOperator(Op):
    def __init__(self, name, log_file_name=None):
        super(SegmentedVideoOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)

    @staticmethod
    def setup_streams(input_streams, filter_name):
        input_streams.filter_name(filter_name)\
            .add_callback(SegmentedVideoOperator.display_frame)
        return []

    def display_frame(self, msg):
        frame_array = labels_to_cityscapes_palette(msg.data)
        img = Image.fromarray(np.uint8(frame_array)).convert('RGB')
        open_cv_image = np.array(img)
        cv2.imshow(self.name, open_cv_image)
        cv2.waitKey(1)

    def execute(self):
        self.spin()
