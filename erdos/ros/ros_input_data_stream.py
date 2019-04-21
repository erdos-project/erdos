import logging
import pickle
import time
from absl import flags

import rospy
from std_msgs.msg import String

from erdos.data_stream import DataStream
from erdos.message import WatermarkMessage

FLAGS = flags.FLAGS

logger = logging.getLogger(__name__)


class ROSInputDataStream(DataStream):
    def __init__(self, op, data_stream):
        super(ROSInputDataStream, self).__init__(
            data_type=data_stream.data_type,
            name=data_stream.name,
            labels=data_stream.labels,
            callbacks=data_stream.callbacks,
            completion_callbacks=data_stream.completion_callbacks,
            uid=data_stream.uid)
        self.op = op

    def setup(self):
        """Initializes a ROS subscriber."""
        # Populate the map with the correct stream names.
        for input_stream in self.op.input_streams:
            self.op._stream_to_high_watermark[input_stream.name] = None

        data_type = self.data_type if self.data_type else String
        # TODO(ionel): We currently transform messages to Strings because
        # we want to pass timestamp and stream info along with the message.
        # However, the extra serialization can add overheads. Fix!
        if FLAGS.ros_non_dropping:
            rospy.Subscriber(self.uid,
                             String,
                             callback=self._on_msg,
                             queue_size=None)
        else:
            rospy.Subscriber(
                self.uid,
                String,
                callback=self._on_msg,
                queue_size=100,
                buff_size=314572800)  # 100 x avg message size (assumed 3MB)

    def _on_msg(self, msg):
        #data = msg if self.data_type else pickle.loads(msg.data)
        msg = pickle.loads(msg.data)
        self.op.log_event(time.time(), msg.timestamp,
                          'receive {}'.format(self.name))
        if isinstance(msg, WatermarkMessage):
            if msg.stream_name in self.op._stream_ignore_watermarks:
                # TODO(yika): big HACK on ignoring watermark sent on stream
                # with label 'no_watermark' = true
                return
            # Ensure that the watermark is monotonically increasing.
            high_watermark = self.op._stream_to_high_watermark[msg.stream_name]
            if not high_watermark:
                # The first watermark, just set the dictionary with the value.
                self.op._stream_to_high_watermark[
                    msg.stream_name] = msg.timestamp
            else:
                if high_watermark >= msg.timestamp:
                    raise Exception(
                        "The watermark received in the msg {} is not "
                        "higher than the watermark previously received "
                        "on the same stream: {}".format(msg, high_watermark))
                else:
                    self.op._stream_to_high_watermark[msg.stream_name] = \
                            msg.timestamp

            # Now check if all other streams have a higher or equal watermark.
            # If yes, flow this watermark. If not, return from this function
            # Also, maintain the lowest watermark observed.
            low_watermark = msg.timestamp
            for stream, watermark in self.op._stream_to_high_watermark.items():
                # TODO(yika): big HACK on ignoring watermark sent on stream
                # with label 'no_watermark' = true
                if (stream not in self.op._stream_ignore_watermarks and
                    stream != msg.stream_name):
                    if not watermark or watermark < msg.timestamp:
                        return
                    if low_watermark > watermark:
                        low_watermark = watermark
            msg = WatermarkMessage(low_watermark)

            # Checkpoint.
            # Note: For correctness reasons, we can only flow watermarks after
            # we checkpoint.
            if (self.op._checkpoint_enable and
                self.op.checkpoint_condition(msg.timestamp)):
                self.op._checkpoint(msg.timestamp)

            # Call the required callbacks.
            for on_watermark_callback in self.completion_callbacks:
                on_watermark_callback(self.op, msg)

            # If no completion callbacks are found, let the watermarks flow
            # automatically. If there is a completion callback, let the
            # developer flow the watermarks.
            # TODO (sukritk) :: Either define an API to know when the system
            # has to flow watermarks, or figure out if the developer has already
            # sent a watermark for a timestamp and don't send duplicates.
            if len(self.completion_callbacks) == 0:
                for output_stream in self.op.output_streams.values():
                    output_stream.send(msg)
        else:
            for on_msg_callback in self.callbacks:
                on_msg_callback(self.op, msg)
