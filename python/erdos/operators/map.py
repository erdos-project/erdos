import erdos


class Map(erdos.Operator):
    """Applies the provided function to a message and sends the resulting
    message."""
    def __init__(self, read_stream, write_stream, function):
        read_stream.add_callback(self.callback, [write_stream])
        self.function = function

    def callback(self, msg, write_stream):
        msg = self.function(msg)
        write_stream.send(msg)

    @staticmethod
    def connect(read_streams):
        return [erdos.WriteStream()]
