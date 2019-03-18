import time

from erdos.data_stream import DataStream
from erdos.message import WatermarkMessage


class RayOutputDataStream(DataStream):
    def __init__(self, op, dependant_op_handles, data_stream):
        super(RayOutputDataStream, self).__init__(
            data_type=data_stream.data_type,
            name=data_stream.name,
            labels=data_stream.labels,
            callbacks=data_stream.callbacks,
            uid=data_stream.uid)
        self._op = op
        self._dependant_op_handles = dependant_op_handles
        self._dependant_op_on_msg = None
        self._dependant_op_on_completion = None

    def send(self, msg):
        """Send a message on the stream.
        Invokes sink actors' on_msg remote methods.
        """
        msg.stream_name = self.name
        msg.stream_uid = self.uid
        if isinstance(msg, WatermarkMessage):
            self._op.log_event(time.time(), msg.timestamp,
                           'watermark send {}'.format(self.name))
            for on_completion_func in self._dependant_op_on_completion:
                on_completion_func.remote(msg)
        else:
            self._op.log_event(time.time(), msg.timestamp,
                               'send {}'.format(self.name))
            for on_msg_func in self._dependant_op_on_msg:
                on_msg_func.remote(msg)

    def setup(self):
        """Setup dependant operator on_msg methods.
        Each ERDOS operator is wrapped in a Ray actor which has a on_msg
        method. This method gets references to the on_msg methods.
        """
        self._dependant_op_on_msg = [
            getattr(actor_handle, "on_msg")
            for actor_handle in self._dependant_op_handles
        ]
        self._dependant_op_on_completion = [
            getattr(actor_handle, "on_completion_msg")
            for actor_handle in self._dependant_op_handles
        ]
