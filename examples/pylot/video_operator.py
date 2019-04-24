import cv2

from erdos.op import Op
from erdos.utils import setup_logging
from utils import add_timestamp, is_camera_stream


class VideoOperator(Op):
    def __init__(self, name, flags, log_file_name=None):
        super(VideoOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._flags = flags
        self._last_seq_num = -1

    @staticmethod
    def setup_streams(input_streams, filter_name=None):
        if filter_name:
            input_streams = input_streams.filter_name(filter_name)
        input_streams.filter(is_camera_stream).add_callback(
            VideoOperator.display_frame)
        return []

    def display_frame(self, msg):
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        frame = msg.data
        add_timestamp(msg.timestamp, frame)
        cv2.imshow(self.name, frame)
        cv2.waitKey(1)

    def execute(self):
        self.spin()
