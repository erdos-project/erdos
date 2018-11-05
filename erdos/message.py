class Message(object):
    """Class used to wrap ERDOS message data.

       Attributes:
           data: The data of the message.
           timestamp (Timestamp): The timestamp of the message.
    """

    def __init__(self, data, timestamp, stream_name='default'):
        self.data = data
        self.timestamp = timestamp
        self.stream_name = stream_name

    def __str__(self):
        return '{{stream: {}, timestamp: {}, data: {}}}'.format(
            self.stream_name, self.timestamp, self.data)
