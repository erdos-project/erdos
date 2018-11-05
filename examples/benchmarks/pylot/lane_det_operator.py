import cv2
from cv_bridge import CvBridge

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging
import pylot_utils


class LaneDetOperator(Op):
    def __init__(self, name, min_runtime_us=None, max_runtime_us=None):
        super(LaneDetOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us
        self._bridge = CvBridge()

    @staticmethod
    def setup_streams(input_streams, op_name):
        def is_rgb_camera_stream(stream):
            return stream.labels.get('camera_type', '') == 'RGB'

        input_streams.filter(is_rgb_camera_stream) \
                     .add_callback(LaneDetOperator.on_frame)
        # TODO(ionel): Specify output type
        return [
            DataStream(
                name='{}_output'.format(op_name), labels={'lanes': 'true'})
        ]

    def on_frame(self, msg):
        self._logger.info('%s received frame %s', self.name, msg.timestamp)
        cv_img = self._bridge.imgmsg_to_cv2(msg.data, "bgr8")
        pylot_utils.do_work(self._logger, self._min_runtime, self._max_runtime)
        self._logger.info('%s publishing lane %s', self.name, msg.timestamp)
        # TODO(ionel): Populate output msg.


#        output_msg = Message(, msg.timestamp)
#        self.get_output_stream(self._output_stream_name).send(output_msg)

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.spin()
