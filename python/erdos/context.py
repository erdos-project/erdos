import erdos
from erdos.timestamp import Timestamp
from erdos.internal import PyTimestamp
from erdos.streams import WriteStream


class SinkContext:
    """A :py:class:`SinkContext` instance enables developers to retrieve
    metadata about the current invocation of either a message or a watermark
    callback in a :py:class:`.Sink` operator.

    Attributes:
        timestamp (:py:class:`.Timestamp`): The timestamp of the current
            invocation of the callback.
        config (:py:class:`.OperatorConfig`): The operator config generated by
            the driver upon connection of the operator to the graph.
    """

    def __init__(self, timestamp: PyTimestamp, config: "erdos.OperatorConfig"):
        self.timestamp = Timestamp(_py_timestamp=timestamp)
        self.config = config

    def __str__(self):
        return "SinkContext(Timestamp={}, Config={})".format(
            self.timestamp, self.config)


class OneInOneOutContext:
    """A :py:class:`OneInOneOutContext` instance enables developers to retrieve
    metadata about the current invocation of either a message or a watermark
    callback in a :py:class:`.OneInOneOut` operator.

    Attributes:
        timestamp (:py:class:`.Timestamp`): The timestamp of the current
            invocation of the callback.
        config (:py:class:`.OperatorConfig`): The operator config generated by
            the driver upon connection of the operator to the graph.
        write_stream (:py:class:`.WriteStream`): The write stream to send
            results to downstream operators.
    """

    def __init__(
        self,
        timestamp: PyTimestamp,
        config: "erdos.OperatorConfig",
        write_stream: WriteStream,
    ):
        self.timestamp = Timestamp(_py_timestamp=timestamp)
        self.config = config
        self.write_stream = write_stream

    def __str__(self):
        return "OneInOneOutContext(Timestamp={}, Config={}, WriteStream={})".\
                format(self.timestamp, self.config, self.write_stream.name)


class OneInTwoOutContext:
    """A :py:class:`OneInTwoOutContext` instance enables developers to retrieve
    metadata about the current invocation of either a message or a watermark
    callback in a :py:class:`.OneInTwoOut` operator.

    Attributes:
        timestamp (:py:class:`.Timestamp`): The timestamp of the current
            invocation of the callback.
        config (:py:class:`.OperatorConfig`): The operator config generated
            by the driver upon connection of the operator to the graph.
        left_write_stream (:py:class:`.WriteStream`): The first write stream to
            send results to downstream operators.
        right_write_stream (:py:class:`.WriteStream`): The second write stream
            to send results to downstream operators.
    """

    def __init__(
        self,
        timestamp: PyTimestamp,
        config: "erdos.OperatorConfig",
        left_write_stream: WriteStream,
        right_write_stream: WriteStream,
    ):
        self.timestamp = Timestamp(_py_timestamp=timestamp)
        self.config = config
        self.left_write_stream = left_write_stream
        self.right_write_stream = right_write_stream

    def __str__(self):
        return "OneInTwoOutContext(Timestamp={}, Config={}, \
                Left WriteStream={}, Right WriteStream={})".format(
            self.timestamp,
            self.config,
            self.left_write_stream.name,
            self.right_write_stream.name,
        )


class TwoInOneOutContext:
    """A :py:class:`TwoInOneOutContext` instance enables developers to retrieve
    metadata about the current invocation of either a message or a watermark
    callback in a :py:class:`.TwoInOneOut` operator.

    Attributes:
        timestamp (:py:class:`.Timestamp`): The timestamp of the current
            invocation of the callback.
        config (:py:class:`.OperatorConfig`): The operator config generated by
            the driver upon connection of the operator to the graph.
        write_stream (:py:class:`.WriteStream`): The write stream to send
            results to downstream operators.
    """

    def __init__(
        self,
        timestamp: PyTimestamp,
        config: "erdos.OperatorConfig",
        write_stream: WriteStream,
    ):
        self.timestamp = Timestamp(_py_timestamp=timestamp)
        self.config = config
        self.write_stream = write_stream

    def __str__(self):
        return "TwoInOneOutContext(Timestamp={}, Config={}, WriteStream={})"\
                .format(
                    self.timestamp,
                    self.config,
                    self.write_stream.name,
                )
