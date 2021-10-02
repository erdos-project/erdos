"""Every second, sends the message count to 3 receivers.
One receiver processes messages using a callback,
one uses the blocking read() call,
and one uses the non-blocking try_read() call.
"""

import erdos
import time

from erdos.operator import Source, Sink


class SendOp(Source):
    def __init__(self):
        print("initializing source op")

    def run(self, write_stream):
        count = 0
        while True:
            msg = erdos.Message(erdos.Timestamp(coordinates=[count]), count)
            print("SendOp: sending {msg}".format(msg=msg))
            write_stream.send(msg)

            count += 1
            time.sleep(1)


class CallbackOp(Sink):
    def __init__(self):
        print("initializing callback op")

    def run(self, read_stream):
        print("Executing run with the ReadStream {}".format(read_stream))

    def on_data(self, context, data):
        print("CallbackOp: received {}".format(data))


class PullOp(Sink):
    def __init__(self):
        print("initializing pull op using read")

    def run(self, read_stream):
        while True:
            data = read_stream.read()
            print("PullOp: received {data}".format(data=data))


class TryPullOp(Sink):
    def __init__(self):
        print("initializing pull op using try_read")

    def run(self, read_stream):
        while True:
            data = read_stream.try_read()
            print("TryPullOp: received {data}".format(data=data))
            time.sleep(0.5)


def main():
    """Creates and runs the dataflow graph."""
    count_stream = erdos.connect_source(SendOp,
                                        erdos.operator.OperatorConfig())
    erdos.connect_sink(CallbackOp, erdos.operator.OperatorConfig(),
                       count_stream)
    erdos.connect_sink(PullOp, erdos.operator.OperatorConfig(), count_stream)
    erdos.connect_sink(TryPullOp, erdos.operator.OperatorConfig(),
                       count_stream)

    erdos.run()


if __name__ == "__main__":
    main()
