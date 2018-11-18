from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.utils import setup_logging
import pylot_utils


class MappingOperator(LoggingOp):
    def __init__(self,
                 name,
                 min_runtime_us=None,
                 max_runtime_us=None,
                 buffer_logs=False):
        super(MappingOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us

    @staticmethod
    def setup_streams(input_streams):
        # TODO(ionel): Define output_streams
        return []

    def execute(self):
        self._logger.info('Executing %s', self.name)
        # TODO(ionel): Register callback.
        self.spin()
