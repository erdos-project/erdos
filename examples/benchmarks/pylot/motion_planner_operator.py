import numpy as np

from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.utils import setup_logging
import pylot_utils


class MotionPlannerOperator(LoggingOp):
    def __init__(self,
                 name,
                 min_runtime_us=None,
                 max_runtime_us=None,
                 buffer_logs=False):
        super(MotionPlannerOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us

    @staticmethod
    def setup_streams(input_streams):
        def is_directions_stream(stream):
            return stream.labels.get('directions', '') == 'true'

        def is_fused_stream(stream):
            return stream.labels.get('fused', '') == 'true'

        def is_predictor_stream(stream):
            return stream.labels.get('predictor', '') == 'true'

        def is_lane_detector_stream(stream):
            return stream.labels.get('lanes', '') == 'true'

        def is_traffic_light_stream(stream):
            return stream.labels.get('lights', '') == 'true'

        def is_intersection_stream(stream):
            return stream.labels.get('intersections', '') == 'true'

        def is_traffic_sign_stream(stream):
            return stream.labels.get('signs', '') == 'true'

        input_streams.filter(is_directions_stream) \
                     .add_callback(MotionPlannerOperator.on_directions)
        input_streams.filter(is_fused_stream) \
                     .add_callback(MotionPlannerOperator.on_fused)
        input_streams.filter(is_predictor_stream) \
                     .add_callback(MotionPlannerOperator.on_predictor)
        input_streams.filter(is_lane_detector_stream) \
                     .add_callback(MotionPlannerOperator.on_lane_det)
        input_streams.filter(is_traffic_light_stream) \
                     .add_callback(MotionPlannerOperator.on_light_det)
        input_streams.filter(is_intersection_stream) \
                     .add_callback(MotionPlannerOperator.on_intersection_det)
        input_streams.filter(is_traffic_sign_stream) \
                     .add_callback(MotionPlannerOperator.on_sign_det)

        return [DataStream(name='motion', labels={'control': 'true'})]

    def on_fused(self, msg):
        pass

    def on_predictor(self, msg):
        pass

    def on_lane_det(self, msg):
        pass

    def on_light_det(self, msg):
        pass

    def on_intersection_det(self, msg):
        pass

    def on_sign_det(self, msg):
        pass

    def on_directions(self, msg):
        pylot_utils.do_work(self._logger, self._min_runtime, self._max_runtime)
        control = {
            'steer': np.random.randint(0, 180),
            'throttle': np.random.randint(0, 10),
            'break': np.random.randint(0, 4),
            'hand_break': False,
            'reverse': False
        }
        output_msg = Message(control, msg.timestamp)
        self.get_output_stream('motion').send(output_msg)

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.spin()
