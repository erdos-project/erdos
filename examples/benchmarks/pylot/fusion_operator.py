import os
import sys

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging

from std_msgs.msg import String


class FusionOperator(Op):
    def __init__(self, name):
        super(FusionOperator, self).__init__(name)
        self._logger = setup_logging(self.name, 'pylot.log')
        self._segments = []
        self._objs = []

    @staticmethod
    def setup_streams(input_streams):
        def is_segmented_stream(stream):
            return stream.labels.get('segmented', '') == 'true'

        def is_detector_stream(stream):
            return stream.labels.get('detector', '') == 'true'

        input_streams.filter(is_detector_stream) \
                     .add_callback(FusionOperator.on_msg_obj_stream)
        input_streams.filter(is_segmented_stream) \
                     .add_callback(FusionOperator.on_msg_segment_stream)

        return [
            DataStream(
                data_type=String, name='fusion', labels={'fused': 'true'})
        ]

    def on_msg_segment_stream(self, msg):
        self._logger.info('%s received segment %s', self.name, msg.timestamp)
        self._segments.append(msg)

    def on_msg_obj_stream(self, msg):
        self._logger.info('%s received object %s', self.name, msg.timestamp)
        self._objs.append(msg)

    @frequency(1)
    def fuse(self):
        # TODO(ionel): Finish fusion operator.
        if len(self._segments) > 0 and len(self._objs) > 0:
            self._segments = []
            self._objs = []


#            output_msg = Message('Fusion output', Timestamp(coordinates=[0]))
#            self.get_output_stream('output').send(output_msg)

    def execute(self):
        self._logger.info('Executing %s', self.name)
        self.fuse()
