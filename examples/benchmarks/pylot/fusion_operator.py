from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging
import pylot_utils


class FusionOperator(LoggingOp):
    def __init__(self, name, min_runtime_us, max_runtime_us,
                 buffer_logs=False):
        super(FusionOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us
        self._positions = []
        self._objs = []
        self._point_clouds = []
        self._radar = []
        self._depth_frames = []
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        def is_tracker_stream(stream):
            return stream.labels.get('tracker', '') == 'true'

        def is_positions_stream(stream):
            return stream.labels.get('positions', '') == 'true'

        def is_lidar_stream(stream):
            return stream.labels.get('lidar', '') == 'true'

        def is_radar_stream(stream):
            return stream.labels.get('radar', '') == 'true'

        def is_depth_camera_stream(stream):
            return stream.labels.get('camera_type', '') == 'depth'

        input_streams.filter(is_tracker_stream).add_callback(
            FusionOperator.on_det_obj_msg)

        input_streams.filter(is_positions_stream).add_callback(
            FusionOperator.on_slam_msg)

        input_streams.filter(is_lidar_stream).add_callback(
            FusionOperator.on_lidar_msg)

        input_streams.filter(is_radar_stream).add_callback(
            FusionOperator.on_radar_msg)

        input_streams.filter(is_depth_camera_stream).add_callback(
            FusionOperator.on_depth_frame_msg)

        # TODO(ionel): Set output type.
        return [DataStream(name='fusion', labels={'fused': 'true'})]

    def on_slam_msg(self, msg):
        self._positions.append(msg)

    def on_det_obj_msg(self, msg):
        self._objs.append(msg)

    def on_lidar_msg(self, msg):
        self._point_clouds.append(msg)

    def on_radar_msg(self, msg):
        self._radar.append(msg)

    def on_depth_frame_msg(self, msg):
        self._depth_frames.append(msg)

    @frequency(10)
    def fuse(self):
        if (len(self._positions) > 0 and len(self._objs) > 0
                and len(self._point_clouds) > 0 and len(self._radar) > 0
                and len(self._depth_frames) > 0):
            output_data = [msg.data for msg in self._objs]
            self._positions = []
            self._objs = []
            self._point_clouds = []
            self._radar = []
            self._depth_frames = []
            pylot_utils.do_work(self._logger, self._min_runtime,
                                self._max_runtime)
            msg = Message(output_data, Timestamp(coordinates=[self._cnt]))
            self.get_output_stream('fusion').send(msg)
            self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.fuse()
        self.spin()
