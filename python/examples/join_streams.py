"""Merges messages from two senders based on timestamp.
"""

import time
from typing import Any

import erdos
from erdos.context import TwoInOneOutContext
from erdos.operator import Source, TwoInOneOut
from erdos.streams import WriteStream


class SendOp(Source):
    """Sends `frequency` messages per second."""

    def __init__(self, frequency):
        print("Initializing send op with frequency {}".format(frequency))
        self.frequency = frequency

    def run(self, write_stream: WriteStream):
        count = 0
        while True:
            timestamp = erdos.Timestamp(coordinates=[count])
            msg = erdos.Message(timestamp, count)
            print("{name}: sending {msg}".format(name=self.config.name, msg=msg))
            write_stream.send(msg)

            watermark = erdos.WatermarkMessage(timestamp)
            print(
                "{name}: sending watermark {watermark}".format(
                    name=self.config.name, watermark=watermark
                )
            )
            write_stream.send(watermark)

            count += 1
            time.sleep(1 / self.frequency)


class JoinOp(TwoInOneOut):
    def __init__(self):
        print("Initializing join op")
        self.left_msgs = {}
        self.right_msgs = {}

    def on_left_data(self, context: TwoInOneOutContext, data: Any):
        print("JoinOp: received {data} on left stream".format(data=data))
        self.left_msgs[context.timestamp] = data

    def on_right_data(self, context: TwoInOneOutContext, data: Any):
        print("JoinOp: received {data} on right stream".format(data=data))
        self.right_msgs[context.timestamp] = data

    def on_watermark(self, context: TwoInOneOutContext):
        left_msg = self.left_msgs.pop(context.timestamp)
        right_msg = self.right_msgs.pop(context.timestamp)
        joined_msg = erdos.Message(context.timestamp, (left_msg, right_msg))
        print("JoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
        context.write_stream.send(joined_msg)


def main():
    """Creates and runs the dataflow graph."""
    left_stream = erdos.connect_source(
        SendOp, erdos.operator.OperatorConfig(name="FastSendOp"), frequency=2
    )
    right_stream = erdos.connect_source(
        SendOp, erdos.operator.OperatorConfig(name="SlowSendOp"), frequency=1
    )
    erdos.connect_two_in_one_out(
        JoinOp, erdos.operator.OperatorConfig(name="JoinOp"), left_stream, right_stream
    )

    erdos.run()


if __name__ == "__main__":
    main()
