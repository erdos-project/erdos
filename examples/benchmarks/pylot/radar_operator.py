import random

from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging


class RadarOperator(LoggingOp):
    def __init__(self, name, buffer_logs=False):
        super(RadarOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, op_name):
        # TODO(ionel): Specify output stream type
        return [
            DataStream(
                name='{}_output'.format(op_name), labels={'radar': 'true'})
        ]

    @frequency(30)
    def publish_data(self):
        #               ^
        #               | longitude_dist
        #               |
        #               |
        #  lateral_dist |
        #  <-------------
        obstacle_id = random.randint(0, 30)
        longitude_dist = random.uniform(0, 100)
        lateral_dist = random.uniform(0, 10)
        longitude_vel = random.uniform(0, 30)
        lateral_vel = random.uniform(0, 30)
        data = (obstacle_id, longitude_dist, lateral_dist, longitude_vel,
                lateral_vel)
        output_msg = Message(data, Timestamp(coordinates=[self._cnt]))
        output_name = '{}_output'.format(self.name)
        self.get_output_stream(output_name).send(output_msg)
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.publish_data()
        self.spin()
