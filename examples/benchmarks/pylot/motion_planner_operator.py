import numpy as np

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging
import pylot_utils


class MotionPlannerOperator(Op):
    def __init__(self, name, min_runtime_us=None, max_runtime_us=None):
        super(MotionPlannerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us

    @staticmethod
    def setup_streams(input_streams):
        def is_directions_stream(stream):
            return stream.labels.get('directions', '') == 'true'

        input_streams.filter(is_directions_stream) \
                     .add_callback(MotionPlannerOperator.on_directions)
        return [DataStream(name='motion', labels={'control': 'true'})]

    def on_directions(self, msg):
        self._logger.info('Received directions %s', msg.timestamp)
        pylot_utils.do_work(self._logger, self._min_runtime, self._max_runtime)
        control = {
            'steer': np.random.randint(0, 180),
            'throttle': np.random.randint(0, 10),
            'break': np.random.randint(0, 4),
            'hand_break': False,
            'reverse': False
        }
        self._logger.info('%s published control message %s', self.name,
                          msg.timestamp)
        output_msg = Message(control, msg.timestamp)
        self.get_output_stream('motion').send(output_msg)

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.spin()
