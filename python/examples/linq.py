"""Every second, sends the message count and
LINQ operations are performed on the stream.
"""

import erdos
import time

from erdos.operator import Source
from erdos.streams import WriteStream


class SendOp(Source):
    def __init__(self):
        print("initializing source op")

    def run(self, write_stream: WriteStream):
        count = 0
        while True:
            msg = erdos.Message(erdos.Timestamp(coordinates=[count]), count)
            print("SendOp: sending {msg}".format(msg=msg))
            write_stream.send(msg)
            count += 1
            time.sleep(1)


def main():
    """Creates and runs the dataflow graph."""
    count_stream = erdos.connect_source(SendOp,
                                        erdos.operator.OperatorConfig())

    map_stream = count_stream.map(lambda a: a * 2)
    filtered_stream = map_stream.filter(lambda a: a > 10)
    left_stream, right_stream = filtered_stream.split(lambda a: a < 50)

    erdos.run()


if __name__ == "__main__":
    main()
