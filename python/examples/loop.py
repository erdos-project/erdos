"""Sends a message in a loop, incrementing the data and timestamp on
each iteration.

Dataflow graph:
+--LoopOp--+
|          |
+-----<----+
"""

import erdos
import time
from typing import Any

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

    def on_data(self, context: OneInOneOutContext, data: Any):
        print("LoopOp: received {data}".format(data=data))
        context.timestamp.coordinates[0] += 1
        data += 1
        time.sleep(1)
        print("LoopOp: sending {data}".format(data=data))
        context.write_stream.send(data)


def main():
    """Creates and runs the dataflow graph."""
    loop_stream = erdos.streams.LoopStream()
    stream = erdos.connect_one_in_one_out(LoopOp,
                                          erdos.operator.OperatorConfig(),
                                          loop_stream)
    loop_stream.connect_loop(stream)

    erdos.run()


if __name__ == "__main__":
    main()
