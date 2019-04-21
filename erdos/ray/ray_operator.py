from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray

from erdos.ray.frequency_actor import FrequencyActor
from erdos.ray.ray_input_data_stream import RayInputDataStream
from erdos.ray.ray_output_data_stream import RayOutputDataStream
from erdos.utils import setup_logging
from erdos.message import WatermarkMessage


@ray.remote
class RayOperator(object):
    """Ray actor class used to wrap ERDOS operators.

       Attributes:
           _op_handle: Handle to the ERDOS operator, which the actor wraps.
           _callbacks: A dict storing the callbacks associated to each stream.
           _op_freq_actor: A Ray actor used to trigger periodic tasks/methods.
    """

    def __init__(self, op_handle):
        # Init ERDOS operator.
        try:
            self._op = op_handle.op_cls(op_handle.name, **op_handle.init_args)
            self._op.framework = op_handle.framework
        except TypeError as e:
            if len(e.args) > 0 and e.args[0].startswith("__init__"):
                first_arg = "{0}.{1}".format(op_handle.op_cls.__name__,
                                             e.args[0])
                e.args = (first_arg, ) + e.args[1:]
            raise
        self._input_streams = op_handle.input_streams
        self._output_streams = op_handle.output_streams
        # Handle to the actor
        self._handle = None
        self._callbacks = {}
        self._completion_callbacks = {}

    def on_msg(self, msg):
        """Invokes corresponding callback for stream stream_name."""
        self._op.log_event(time.time(), msg.timestamp,
                           'receive {}'.format(msg.stream_name))
        for cb in self._callbacks.get(msg.stream_uid, []):
            cb(msg)

    def on_completion_msg(self, msg):
        """Invokes corresponding callback for stream stream_name."""
        self._op.log_event(time.time(), msg.timestamp,
                           'receive watermark {}'.format(msg.stream_name))

        # Ensure that the watermark is monotonically increasing.
        high_watermark = self._op._stream_to_high_watermark[msg.stream_name]
        if not high_watermark:
            # The first watermark, just set the dictionary with the value.
            self._op._stream_to_high_watermark[
                msg.stream_name] = msg.timestamp
        else:
            if high_watermark >= msg.timestamp:
                raise Exception(
                    "The watermark received in the msg {} is not "
                    "higher than the watermark previously received "
                    "on the same stream: {}".format(msg, high_watermark))
            else:
                self._op._stream_to_high_watermark[msg.stream_name] = \
                        msg.timestamp

        # Now check if all other streams have a higher or equal watermark.
        # If yes, flow this watermark. If not, return from this function
        # Also, maintain the lowest watermark observed.
        low_watermark = msg.timestamp
        for stream, watermark in self._op._stream_to_high_watermark.items():
            # TODO(yika): big HACK on ignoring watermark sent on stream
            # with label 'no_watermark' = true
            if (stream not in self._op._stream_ignore_watermarks and
                stream != msg.stream_name):
                if not watermark or watermark < msg.timestamp:
                    return
                if low_watermark > watermark:
                    low_watermark = watermark
        new_msg = WatermarkMessage(low_watermark)
        new_msg.stream_uid = msg.stream_uid

        # Checkpoint.
        # Note: For correctness reasons, we can only flow watermarks after
        # we checkpoint.
        if (self._op._checkpoint_enable and
            self._op.checkpoint_condition(msg.timestamp)):
            self._op._checkpoint(msg.timestamp)

        # Call the required callbacks.
        for cb in self._completion_callbacks.get(new_msg.stream_uid, []):
            cb(new_msg)

        # Finished calling the required callbacks. Send the watermark forward
        # to the dependent operators.

        # TODO (sukritk) :: Same issue as erdos/ros/ros_input_data_stream.py
        # TODO (sukritk) FIX (Ray Issue #4463): Remove when Ray issue is fixed.
        if not self._completion_callbacks.get(new_msg.stream_uid):
            watermark_msg = WatermarkMessage(msg.timestamp, msg.stream_name)
            for output_stream in self._op.output_streams.values():
                output_stream.send(watermark_msg)

    def register_callback(self, stream_uid, callback_name):
        """Registers a callback for a given stream."""
        cbs = self._callbacks.get(stream_uid, [])
        self._callbacks[stream_uid] = cbs + [getattr(self._op, callback_name)]

    def register_completion_callback(self, stream_uid, callback_name):
        """Registers a watermark completion callback for a given stream."""
        callbacks = self._completion_callbacks.get(stream_uid, [])
        self._completion_callbacks[stream_uid] = callbacks + [getattr(self._op, callback_name)]

    def on_frequency(self, func_name, *args):
        """Invokes operator func_name.
        Method is called by the frequency actor when a periodic task/method
        must run.
        """
        callback = getattr(self._op, func_name)
        callback(*args)

    def set_handle(self, handle):
        self._handle = handle

    def setup_frequency_actor(self):
        """Creates a Ray frequency actor.
        Each Ray operator has a Ray frequency actor associated with it. The
        actor call on_frequency when periodic methods must execute.
        """
        self._op.freq_actor = FrequencyActor.remote(self._handle)

    def setup_streams(self, dependant_ops_handles):
        """Sets the input_stream.ray_sink to the Ray operator."""
        # Populate the map with the correct stream names.
        for input_stream in self._input_streams:
            self._op._stream_to_high_watermark[input_stream.name] = None

        # Wrap input streams in Ray data streams.
        ray_input_streams = [
            RayInputDataStream(self._handle, input_stream)
            for input_stream in self._input_streams
        ]
        self._op._add_input_streams(ray_input_streams)
        # Wrap output streams in Ray data streams.
        ray_output_streams = [
            RayOutputDataStream(
                self._op,
                dependant_ops_handles.get(output_stream.uid, []),
                output_stream) for output_stream in self._output_streams
        ]
        self._op._add_output_streams(ray_output_streams)
        self._op._internal_setup_streams()

    def execute(self):
        """Executes the operator."""
        self._op.execute()
