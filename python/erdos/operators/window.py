"""
   Windowing Operators will collect incoming inputs into groups.
   Messages sent from these operators take the form of: 
   (most recent message's timestamp, list of Messages (msg.timestamp, msg.data) ).
"""

class TumblingWindowOp(erdos.Operator):
    """Windows incoming messages into non-overlapping lists of `window_size`."""
    def __init__(self, read_stream, write_stream, window_size):
        read_stream.add_callback(self.callback, [write_stream])
        self.window_size = window_size
        self.elems = []

    def callback(self, msg, write_stream):
        print("TumblingWindowOp: received {msg}".format(msg=msg))
        self.elems.append(msg)
        if len(self.elems) == self.window_size:
            msg = erdos.Message(msg.timestamp, self.elems)
            write_stream.send(msg)

            self.elems = []
            print("TumblingWindowOp: sending {msg}".format(msg=msg))

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

class SlidingWindowOp(erdos.Operator):
    """Windows incoming messages into overlapping lists of `window_size`,
       with windows being separated by messages that are sent `offset` timestamps apart.
    """
    def __init__(self, read_stream, write_stream, window_size, offset):
        read_stream.add_callback(self.callback, [write_stream])
        self.window_size = window_size
        self.offset = offset
        self.elems = []
        self.count = 0

    def callback(self, msg, write_stream):
        print("SlidingWindowOp: received {msg}".format(msg=msg))
        self.elems.append(msg)
        
        if len(self.elems) >= self.window_size:
            msg = erdos.Message(msg.timestamp, self.elems[0:self.window_size])
            write_stream.send(msg)

            self.elems = self.elems[self.offset:]
            print("SlidingWindowOp: sending {msg}".format(msg=msg))

        self.count += 1

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

class WatermarkWindowOp(erdos.Operator):
    """Sends a window of messages when a watermark has been received.
       Messages are collected since time of first message or since time of last watermark.
    """
    def __init__(self, read_stream, write_stream):
        read_stream.add_callback(self.callback, [write_stream])
        erdos.add_watermark_callback([read_stream], [write_stream], self.watermark_callback)
        self.elems = []

    def callback(self, msg, write_stream):
        print("WatermarkWindowOp: received {msg}".format(msg=msg))
        self.elems.append(msg)

    def watermark_callback(self, watermark, write_stream):
        print("WatermarkWindowOp: received watermark {watermark}".format(watermark=watermark))
        msg = erdos.Message(watermark, self.elems)
        write_stream.send(msg)
        self.elems = []
        print("WatermarkWindowOp: sending {msg}".format(msg=msg))

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]