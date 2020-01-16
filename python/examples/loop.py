"""Sends a message in a loop, incrementing the data and timestamp on
each iteration.

Dataflow graph:
+--LoopOp--+
|          |
+-----<----+
"""

import erdos
import time


class LoopOp(erdos.Operator):
    def __init__(self, read_stream, write_stream):
        self.write_stream = write_stream
        read_stream.add_callback(LoopOp.callback, [write_stream])

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

    @staticmethod
    def callback(msg, write_stream):
        print(f"LoopOp: received {msg}")
        msg.timestamp.coordinates[0] += 1
        msg.data += 1
        time.sleep(1)
        print(f"LoopOp: sending {msg}")
        write_stream.send(msg)

    def run(self):
        msg = erdos.Message(erdos.Timestamp(coordinates=[0]), 0)
        print(f"LoopOp: sending {msg}")
        self.write_stream.send(msg)


def driver():
    """Creates the dataflow graph."""
    loop_stream = erdos.LoopStream()
    (stream, ) = erdos.connect(LoopOp, [loop_stream])
    loop_stream.set(stream)


if __name__ == "__main__":
    erdos.run(driver)
