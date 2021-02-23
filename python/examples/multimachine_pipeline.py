"""Every second, sends the message count to 2 receivers.
One receiver processes messages using a callback,
one uses the blocking read() call.
"""

import erdos
import time


class SendOp(erdos.Operator):
    def __init__(self, write_stream):
        self.write_stream = write_stream

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = 0
        while True:
            msg = erdos.Message(erdos.Timestamp(coordinates=[count]), count)
            print("SendOp: sending {msg}".format(msg=msg))
            self.write_stream.send(msg)

            count += 1
            time.sleep(1)


class CallbackOp(erdos.Operator):
    def __init__(self, read_stream):
        print("initializing  op")
        read_stream.add_callback(CallbackOp.callback)

    @staticmethod
    def callback(msg):
        print("CallbackOp: received {msg}".format(msg=msg))

    @staticmethod
    def connect(read_streams):
        return []


class PullOp(erdos.Operator):
    def __init__(self, read_stream):
        self.read_stream = read_stream

    @staticmethod
    def connect(read_streams):
        return []

    def run(self):
        while True:
            data = self.read_stream.read()
            print("PullOp: received {data}".format(data=data))


def main():
    """Creates and runs the dataflow graph."""
    (count_stream, ) = erdos.connect(SendOp, erdos.OperatorConfig(addr="172.17.0.9"), [])
    erdos.connect(CallbackOp, erdos.OperatorConfig(addr="172.17.0.10"), [count_stream])
    erdos.connect(PullOp, erdos.OperatorConfig(addr="172.17.0.11"), [count_stream])

    erdos.run()


if __name__ == "__main__":
    main()
