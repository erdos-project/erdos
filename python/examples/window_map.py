"""Windows messages from one sender based on user-defined window_size.
Separate operator applies a map function on sent windows.
"""

from erdos.operators import window, map
import erdos
import time


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

            if count % 3 == 0:
                watermark = erdos.WatermarkMessage(timestamp)
                print("{name}: sending watermark {watermark}".format(
                    name=self.config.name, watermark=watermark))
                self.write_stream.send(watermark)

            count += 1
            time.sleep(1 / self.frequency)

def main():
    """Creates and runs the dataflow graph."""

    def add(Msg):
        """Mapping Function passed into MapWindowOp,
           returns sums all the messages' data."""
        total = 0
        for msg in Msg.data:
            total += msg.data
        return erdos.Message(Msg.timestamp, total)

    (source_stream, ) = erdos.connect(SendOp,
                                    erdos.OperatorConfig(name="SendOp"),
                                    [],
                                    frequency=1)
    (window_stream, ) = erdos.connect(window.WatermarkWindow, erdos.OperatorConfig(),
                                    [source_stream])
    (map_stream, ) = erdos.connect(map.Map, erdos.OperatorConfig(),
                                    [window_stream], function=add)
    erdos.run()


if __name__ == "__main__":
    main()
