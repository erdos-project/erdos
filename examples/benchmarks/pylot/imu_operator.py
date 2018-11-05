import random

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging
import pylot_utils


class IMUOperator(Op):
    def __init__(self, name):
        super(IMUOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        # TODO(ionel): Define output type.
        return [DataStream(name='imu', labels={'IMU': 'true'})]

    @frequency(50)
    def publish_imu_data(self):
        roll = random.random()
        pitch = random.random()
        yaw = random.random()
        self._logger.info('%s publishing IMU data %d', self.name, self._cnt)
        output_msg = Message((roll, pitch, yaw),
                             Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('imu').send(output_msg)
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.publish_imu_data()
