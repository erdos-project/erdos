"""Windows messages from one sender based on user-defined window_size.
Separate operator applies a map function on sent windows.
"""

import time

import erdos
from erdos.operators import window, map


class SendOp(erdos.Operator):
    """Sends `frequency` messages per second."""

    def __init__(self, write_stream, frequency):
        self.frequency = frequency
        self.write_stream = write_stream

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = 0
        while True:
            timestamp = erdos.Timestamp(coordinates=[count])
            msg = erdos.Message(timestamp, count)
            print("{name}: sending {msg}".format(name=self.config.name,
                                                 msg=msg))
            self.write_stream.send(msg)

            count += 1
            time.sleep(1 / self.frequency)


def main():
    """Creates and runs the dataflow graph."""

    def add(msg):
        """Mapping Function passed into MapOp,
           returns a new Message that sums the data of each message in
           msg.data."""
        total = 0
        for i in msg.data:
            total += i.data
        return erdos.Message(msg.timestamp, total)

    (source_stream, ) = erdos.connect(SendOp,
                                      erdos.OperatorConfig(name="SendOp"), [],
                                      frequency=3)
    (window_stream, ) = erdos.connect(window.TumblingWindow,
                                      erdos.OperatorConfig(), [source_stream],
                                      window_size=3)
    (map_stream, ) = erdos.connect(map.Map,
                                   erdos.OperatorConfig(), [window_stream],
                                   function=add)
    extract_stream = erdos.ExtractStream(map_stream)

    erdos.run_async()

    while True:
        recv_msg = extract_stream.read()
        print("ExtractStream: received {recv_msg}".format(recv_msg=recv_msg))


if __name__ == "__main__":
    main()
