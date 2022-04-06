"""Sends a message in a loop, incrementing the data and timestamp on
each iteration.

Dataflow graph:
+--LoopOp--+
|          |
+-----<----+
"""

import time

import erdos
from erdos.context import OneInOneOutContext
from erdos.operator import OneInOneOut
from erdos.streams import ReadStream, WriteStream


class LoopOp(OneInOneOut):
    def __init__(self):
        print("initializing loop op")

    def run(self, read_stream: ReadStream, write_stream: WriteStream):
        msg = erdos.Message(erdos.Timestamp(coordinates=[0]), 0)
        print("LoopOp: sending {msg}".format(msg=msg))
        write_stream.send(msg)

    def on_data(self, context: OneInOneOutContext, data: int):
        print("LoopOp: received {data}".format(data=data))
        time.sleep(1)
        # Update data and timestamp.
        data += 1
        coordinates = list(context.timestamp.coordinates)
        coordinates[0] += 1
        timestamp = erdos.Timestamp(coordinates=coordinates)
        message = erdos.Message(timestamp, data)
        print("LoopOp: sending {message}".format(message=message))
        context.write_stream.send(message)


def main():
    """Creates and runs the dataflow graph."""
    loop_stream = erdos.streams.LoopStream()
    stream = erdos.connect_one_in_one_out(
        LoopOp, erdos.operator.OperatorConfig(), loop_stream
    )
    loop_stream.connect_loop(stream)

    erdos.run()


if __name__ == "__main__":
    main()
