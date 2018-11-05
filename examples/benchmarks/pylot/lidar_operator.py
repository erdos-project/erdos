import numpy as np
import os
from open3d import read_point_cloud

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging

NUM_POINT_CLOUDS = 50
# TODO(ionel): Getting paths like this is dangerous. Fix!
point_clouds_path = os.path.dirname(
    os.path.dirname(os.path.realpath(__file__))) + '/../pylot/images/'


class LidarOperator(Op):
    def __init__(self, name, num_points=100000):
        super(LidarOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._num_points = num_points
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, op_name):
        # TODO(ionel): Specify output type.
        return [
            DataStream(
                name='{}_output'.format(op_name), labels={'lidar': 'true'})
        ]

    @frequency(10)
    def publish_point_cloud(self):
        pc = self.create_point_cloud(self._num_points)
        # pc = self.read_point_cloud(self._cnt % NUM_POINT_CLOUDS)
        self._logger.info('%s publishing point cloud %d', self.name, self._cnt)
        output_msg = Message(pc, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('{}_output'.format(self.name)).send(output_msg)
        self._cnt += 1

    def create_point_cloud(self, num_points):
        return np.zeros((num_points, 3), dtype=np.uint16)

    def read_point_cloud(self, cnt):
        pc = read_point_cloud('{}carla{}.ply'.format(point_clouds_path, cnt))
        return pc

    def execute(self):
        """Operator entry method.
        The method implements the operator logic.
        """
        self._logger.info('Executing %s', self.name)
        self.publish_point_cloud()
