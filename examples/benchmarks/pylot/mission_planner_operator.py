import numpy as np

from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging
import pylot_utils


class MissionPlannerOperator(LoggingOp):
    def __init__(self,
                 name,
                 min_runtime_us=None,
                 max_runtime_us=None,
                 buffer_logs=False):
        super(MissionPlannerOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        def is_positions_stream(stream):
            return stream.labels.get('positions', '') == 'true'

        input_streams.filter(is_positions_stream).add_callback(
            MissionPlannerOperator.on_position_msg)

        # TODO(ionel): Specify output type.
        return [DataStream(name='directions', labels={'directions': 'true'})]

    def on_position_msg(self, msg):
        self._position = msg.data

    @frequency(1)
    def calculate_directions(self):
        pylot_utils.do_work(self._logger, self._min_runtime, self._max_runtime)
        # Send value 0-5 for direction (e.g., 0 left, 1 right, ...).
        direction = np.random.randint(0, 6)
        output_msg = Message(direction, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('directions').send(output_msg)
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.calculate_directions()
        self.spin()
