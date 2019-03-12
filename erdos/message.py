class Message(object):
    """Class used to wrap ERDOS message data.

       Attributes:
           data: The data of the message.
           timestamp (Timestamp): The timestamp of the message.
    """

    def __init__(self, data, timestamp, stream_name='default', watermark = False):
        self.data = data
        self.timestamp = timestamp
        self.stream_name = stream_name
        self.watermark = watermark

    def __str__(self):
        return '{{stream: {}, timestamp: {}, watermark: {}, data: {}}}'.format(
            self.stream_name, self.timestamp, self.watermark, self.data)
