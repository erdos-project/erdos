from erdos.data_stream import DataStream


class RayOutputDataStream(DataStream):
    def __init__(self, dependant_op_handles, data_stream):
        super(RayOutputDataStream, self).__init__(
            data_type=data_stream.data_type,
            name=data_stream.name,
            labels=data_stream.labels,
            callbacks=data_stream.callbacks,
            uid=data_stream.uid)
        self._dependant_op_handles = dependant_op_handles
        self._dependant_op_on_msg = None

    def send(self, msg):
        """Send a message on the stream.
        Invokes sink actors' on_msg remote methods.
        """
        msg.stream_uid = self.uid
        for on_msg_func in self._dependant_op_on_msg:
            on_msg_func.remote(msg)

    def setup(self):
        """Setup dependant operator on_msg methods.
        Each ERDOS operator is wrapped in a Ray actor which has a on_msg method.
        This method gets references to the on_msg methods.
        """
        self._dependant_op_on_msg = [
            getattr(actor_handle, "on_msg")
            for actor_handle in self._dependant_op_handles
        ]
