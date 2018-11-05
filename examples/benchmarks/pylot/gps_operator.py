import random

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging
import pylot_utils


class GPSOperator(Op):
    def __init__(self, name):
        super(GPSOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        # TODO(ionel): Define output type.
        return [DataStream(name='gps_coordinates', labels={'GPS': 'true'})]

    @frequency(50)
    def publish_coordinates(self):
        coords = (random.uniform(0, 180), random.uniform(0, 180))
        self._logger.info('%s publishing GPS coordinates %d', self.name,
                          self._cnt)
        output_msg = Message(coords, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('gps_coordinates').send(output_msg)
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.publish_coordinates()
