from erdos.data_stream import DataStream
from erdos.logging_op import LoggingOp
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging
import pylot_utils


class PredictionOperator(LoggingOp):
    def __init__(self,
                 name,
                 min_runtime_us=None,
                 max_runtime_us=None,
                 buffer_logs=False):
        super(PredictionOperator, self).__init__(name, buffer_logs)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._min_runtime = min_runtime_us
        self._max_runtime = max_runtime_us
        self._objs = []
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        def is_bbox_stream(stream):
            return stream.labels.get('type', '') == 'bbox'

        input_streams.filter(is_bbox_stream).add_callback(
            PredictionOperator.on_det_objs_msg)

        return [
            DataStream(name='prediction_stream', labels={'predictor': 'true'})
        ]

    def on_det_objs_msg(self, msg):
        self._objs.append(msg.data)

    @frequency(1)
    def predict(self):
        predicted_locs = self._objs + self._objs + self._objs
        pylot_utils.do_work(self._logger, self._min_runtime, self._max_runtime)
        output_msg = Message(predicted_locs,
                             Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('prediction_stream').send(output_msg)
        self._objs = []
        self._cnt += 1

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.predict()
        self.spin()
