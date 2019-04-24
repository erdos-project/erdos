class DataStream(object):
    """Data stream base class.

    Attributes:
        data_type: The type of the stream messages. This is required because
            ROS subscribers and publishers must specify a data type.
        name (str): A unique string naming the stream.
        labels (dict: str -> str): Describes properties of the data stream.
    """

    def __init__(self,
                 data_type=None,
                 name="",
                 labels=None,
                 callbacks=None,
                 completion_callbacks=None,
                 uid=None):
        self.name = name if name else "{0}_{1}".format(self.__class__.__name__,
                                                       hash(self))
        self.data_type = data_type
        self._uid = uid

        if labels:  # both keys and values in a label must be a single string
            for k, v in labels.items():
                assert type(k) == str, 'label key type must be str'
                assert type(v) == str, 'label value type must be str'
            self.labels = labels
        else:
            self.labels = {}
        if callbacks is None:
            self.callbacks = set([])
        else:
            self.callbacks = callbacks

        if completion_callbacks is None:
            self.completion_callbacks = set([])
        else:
            self.completion_callbacks = completion_callbacks

    def add_callback(self, on_msg_cb):
        """Registers a stream callback.

        Args:
            on_msg_cb (Message -> None): Callback to be invoked upon the
                receipt of a message.
        """
        self.callbacks.add(on_msg_cb)

    def add_completion_callback(self, on_watermark_cb):
        """ Registers a watermark callback.

        Args:
            on_watermark_cb (Message -> None): Callback to be invoked upon
                the completion of a timestamp.
        """
        self.completion_callbacks.add(on_watermark_cb)

    def send(self, msg):
        """Send a message on the stream.

        Args:
            msg (Message): the mesage to send.
        """
        raise NotImplementedError("DataStream does not implement send.")

    def setup(self):
        """Configures how this stream communicate with other streams"""
        raise NotImplementedError("DataStream does not implement setup.")

    def _copy_stream(self):
        """Transforms the OutputStream into an InputStream"""
        return DataStream(
            data_type=self.data_type,
            name=self.name,
            labels=self.labels.copy(),
            callbacks=self.callbacks.copy(),
            uid=self.uid)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        # TODO(ionel): Might want to return the UID here.
        return self.name

    def get_label(self, key):
        """
        Retrieves the label for the given key. Returns None if the key
        is not present.
        """
        return self.labels.get(key, None)

    @property
    def uid(self):
        if self._uid is None:
            raise ValueError("Stream uid is None.")
        return self._uid

    @uid.setter
    def uid(self, sender_op_id):
        self._uid = "{}/{}".format(sender_op_id, self.name)
