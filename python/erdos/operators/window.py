"""Windowing Operators will collect incoming inputs into groups.

Messages sent from these operators contain a list of Messages as data
and have a timestamp corresponding to the most recent Message in the list.
"""

import erdos


class TumblingWindow(erdos.Operator):
    """Windows incoming messages into non-overlapping lists of `window_size`.
    """
    def __init__(self, read_stream, write_stream, window_size):
        read_stream.add_callback(self.callback, [write_stream])
        self.window_size = window_size
        self.msgs = []

    def callback(self, msg, write_stream):
        self.msgs.append(msg)
        if len(self.msgs) == self.window_size:
            msg = erdos.Message(msg.timestamp, self.msgs)
            write_stream.send(msg)

            self.msgs = []

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]


class SlidingWindow(erdos.Operator):
    """Windows incoming messages into overlapping lists of `window_size`.

    Windows are separated by messages that are sent 'offset' timestamps apart.
    """
    def __init__(self, read_stream, write_stream, window_size, offset):
        read_stream.add_callback(self.callback, [write_stream])
        self.window_size = window_size
        self.offset = offset
        self.msgs = []
        self.count = 0

    def callback(self, msg, write_stream):
        self.msgs.append(msg)

        if len(self.msgs) >= self.window_size:
            msg = erdos.Message(msg.timestamp, self.msgs[0:self.window_size])
            write_stream.send(msg)

            self.msgs = self.msgs[self.offset:]

        self.count += 1

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]


class WatermarkWindow(erdos.Operator):
    """Sends a window of messages when a watermark has been received.

    Messages are collected since time of first message or since time of last
    watermark.
    """
    def __init__(self, read_stream, write_stream):
        read_stream.add_callback(self.callback, [write_stream])
        erdos.add_watermark_callback([read_stream], [write_stream],
                                     self.watermark_callback)
        self.msgs = []

    def callback(self, msg, write_stream):
        self.msgs.append(msg)

    def watermark_callback(self, watermark, write_stream):
        msg = erdos.Message(watermark, self.msgs)
        write_stream.send(msg)
        self.msgs = []

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]
