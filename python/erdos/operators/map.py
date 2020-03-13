class MapWindowOp(erdos.Operator):
    """Operator transforms a given list of Messages using a user-defined function"""

    def __init__(self, read_stream, write_stream, function):
        read_stream.add_callback(self.callback, [write_stream])
        self.function = function
    
    def callback(self, msg, write_stream):
        """Operator expects msg.data to be a list of Messages"""
        print("MapOp: received {msg}".format(msg=msg))
        msg=erdos.Message(msg.timestamp, self.function(msg.data))
        print("MapOp: sending {msg}".format(msg=msg))
        write_stream.send(msg)

    @staticmethod
    def connect(read_streams):
        return [erdos.WriteStream()]