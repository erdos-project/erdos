import os

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging

NUM_FRAMES = 50
# TODO(ionel): Getting paths like this is dangerous. Fix!
frames_path = os.path.dirname(os.path.dirname(
    os.path.realpath(__file__))) + '/../pylot/images/'


class DepthCameraOperator(Op):
    def __init__(self, name):
        super(DepthCameraOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, op_name):
        # TODO(ionel): Specify stream type.
        return [
            DataStream(
                name='{}_output'.format(op_name),
                labels={'camera_type': 'depth'})
        ]

    @frequency(10)
    def publish_frame(self):
        image = self.read_image(self._cnt % NUM_FRAMES)
        # TODO(ionel): Implement.
        pass

    def read_image(self, cnt):
        # TODO(ionel): Implement.
        pass

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.publish_frame()
