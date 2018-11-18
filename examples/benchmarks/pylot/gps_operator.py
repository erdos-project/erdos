import random

from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging


class GPSOperator(LoggingOp):
    def __init__(self, name, buffer_logs=False):
        super(GPSOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        # TODO(ionel): Define output type.
        return [DataStream(name='gps_coordinates', labels={'GPS': 'true'})]

    @frequency(50)
    def publish_coordinates(self):
        coords = (random.uniform(0, 180), random.uniform(0, 180))
        output_msg = Message(coords, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('gps_coordinates').send(output_msg)
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.publish_coordinates()
        self.spin()
