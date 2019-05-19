import cv2
import time

from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from pylot_utils import add_timestamp, bgra_to_bgr, create_detected_lane_stream, is_camera_stream


class LaneDetectionOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(LaneDetectionOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._output_stream_name = output_stream_name

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        # Register a callback on the camera input stream.
        input_streams.filter(is_camera_stream).add_callback(
            LaneDetectionOperator.on_msg_camera_stream)
        return [create_detected_lane_stream(output_stream_name)]

    def on_msg_camera_stream(self, msg):
        start_time = time.time()
        assert msg.encoding == 'BGR', 'Expects BGR frames'
        image_np = msg.frame

        # TODO(ionel): Implement lane detection.

        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},"{}",{}'.format(
            time_epoch_ms(), self.name, msg.timestamp, runtime))

        if self._flags.visualize_lane_detection:
            add_timestamp(msg.timestamp, image_np)
            cv2.imshow(self.name, image_np)
            cv2.waitKey(1)

        output_msg = Message(image_np, msg.timestamp)
        self.get_output_stream(self._output_stream_name).send(output_msg)

    def execute(self):
        self.spin()
