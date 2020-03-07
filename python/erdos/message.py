from erdos.timestamp import Timestamp


class Message(object):
    """Class used to wrap ERDOS message data.

       Attributes:
           timestamp (Timestamp): The timestamp of the message.
           data: The data of the message.
    """
    def __init__(self, timestamp, data):
        if not isinstance(timestamp, Timestamp):
            raise TypeError("timestamp must be of type `erdos.Timestamp`")

        self.timestamp = timestamp
        self.data = data

    def __str__(self):
        return "{{timestamp: {}, data: {}}}".format(self.timestamp, self.data)


class WatermarkMessage(Message):
    """Class used to wrap ERDOS watermark message.

       Attributes:
           timestamp (Timestamp): The timestamp for which this is a watermark.
    """
    def __init__(self, timestamp):
        super(WatermarkMessage, self).__init__(timestamp, None)

    def __str__(self):
        return "{{timestamp: {}, watermark: True}}".format(self.timestamp)

    @property
    def is_top(self):
        return self.timestamp.is_top
