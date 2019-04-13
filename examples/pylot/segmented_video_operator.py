import cv2
import numpy as np
import PIL.Image as Image

from carla.image_converter import labels_to_cityscapes_palette

from erdos.op import Op
from erdos.utils import setup_logging


class SegmentedVideoOperator(Op):
    def __init__(self, name, flags, log_file_name=None):
        super(SegmentedVideoOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._flags = flags
        self._last_seq_num = -1

    @staticmethod
    def setup_streams(input_streams, filter_name):
        input_streams.filter_name(filter_name)\
            .add_callback(SegmentedVideoOperator.display_frame)
        return []

    def display_frame(self, msg):
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        frame_array = labels_to_cityscapes_palette(msg.data)
        img = Image.fromarray(np.uint8(frame_array)).convert('RGB')
        open_cv_image = np.array(img)
        cv2.imshow(self.name, open_cv_image)
        cv2.waitKey(1)

    def execute(self):
        self.spin()
