import os
import sys
from open3d import draw_geometries, read_point_cloud

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging


class LidarVisualizerOperator(Op):
    def __init__(self, name):
        super(LidarVisualizerOperator, self).__init__(name)
        self._logger = setup_logging(self.name)
        self.cnt = 0

    @staticmethod
    def setup_streams(input_streams, filter):
        input_streams.filter_name(filter)\
            .add_callback(LidarVisualizerOperator.display_point_cloud)
        return []

    def display_point_cloud(self, msg):
        self._logger.info("Got point cloud {}".format(msg.data.point_cloud))
        #        filename = './carla-point-cloud{}.ply'.format(self.cnt)
        filename = './point_cloud_tmp.ply'
        msg.data.save_to_disk(filename)
        self.cnt += 1
        # TODO(ionel): Do not write to file, directly pass point cloud.
        pcd = read_point_cloud(filename)
        draw_geometries([pcd])

    def execute(self):
        self.spin()
