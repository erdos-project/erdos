from erdos.data_stream import DataStream


class RayInputDataStream(DataStream):
    def __init__(self, actor_handle, data_stream):
        super(RayInputDataStream, self).__init__(
            data_type=data_stream.data_type,
            name=data_stream.name,
            labels=data_stream.labels,
            callbacks=data_stream.callbacks,
            completion_callbacks=data_stream.completion_callbacks,
            uid=data_stream.uid)
        self._actor_handle = actor_handle

    def setup(self):
        for on_msg_callback in self.callbacks:
            self._actor_handle.register_callback.remote(
                self.uid, on_msg_callback)
 
        for on_watermark_callback in self.completion_callbacks:
            self._actor_handle.register_completion_callback.remote(
                self.uid, on_watermark_callback)
