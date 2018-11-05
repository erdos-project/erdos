import random

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging
import pylot_utils


class SLAMOperator(Op):
    def __init__(self, name, min_runtime_us=None, max_runtime_us=None):
        super(SLAMOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us
        self._cnt = 0
        self._point_cloud = None
        self._radar = None
        self._gps = None
        self._imu = None

    @staticmethod
    def setup_streams(input_streams):

        # Filter func
        def is_lidar_stream(stream):
            return stream.labels.get('lidar', '') == 'true'

        def is_radar_stream(stream):
            return stream.labels.get('radar', '') == 'true'

        def is_gps_stream(stream):
            return stream.labels.get('GPS', '') == 'true'

        def is_imu_stream(stream):
            return stream.labels.get('IMU', '') == 'true'

        # Lidar stream
        input_streams.filter(is_lidar_stream) \
                     .add_callback(SLAMOperator.on_point_cloud)
        # Radar stream
        input_streams.filter(is_radar_stream) \
                     .add_callback(SLAMOperator.on_radar)
        # GPS stream
        input_streams.filter(is_gps_stream) \
                     .add_callback(SLAMOperator.on_gps)
        # IMU stream
        input_streams.filter(is_imu_stream)\
                     .add_callback(SLAMOperator.on_imu)
        # TODO(ionel): Specify output stream type
        return [DataStream(name='location', labels={'positions': 'true'})]

    def on_point_cloud(self, msg):
        self._logger.info('%s received point cloud %s', self.name,
                          msg.timestamp)
        self._point_cloud = msg.data

    def on_radar(self, msg):
        self._logger.info('%s received radar %s', self.name, msg.timestamp)
        self._radar = msg.data

    def on_gps(self, msg):
        self._logger.info('%s received GPS %s', self.name, msg.timestamp)
        self._gps = msg.data

    def on_imu(self, msg):
        self._logger.info('%s received IMU %s', self.name, msg.timestamp)
        self._imu = msg.data

    @frequency(50)
    def localize(self):
        self._logger.info('%s localize called %d', self.name, self._cnt)
        pylot_utils.do_work(self._logger, self._min_runtime, self._max_runtime)
        # TODO(ionel): Check how synchronized the data is.
        # TODO(ionel): Interact with the mapping operator.
        location = (random.uniform(0, 180), random.uniform(0, 180))
        self._logger.info('%s publish localization data %d', self.name,
                          self._cnt)
        output_msg = Message(location, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('location').send(output_msg)
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.localize()
