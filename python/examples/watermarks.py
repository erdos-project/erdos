"""Every second, sends the message count to the batch operator.
Sends a watermark every 3 messages which releases the batch.
"""

import time
from typing import Any

import erdos
from erdos.context import OneInOneOutContext, SinkContext
from erdos.operator import OneInOneOut, Sink, Source
from erdos.streams import ReadStream, WriteStream


class SendOp(Source):
    def __init__(self):
        print("initializing send op")

    def run(self, write_stream: WriteStream):
        count = 0
        while True:
            timestamp = erdos.Timestamp(coordinates=[count])
            msg = erdos.Message(timestamp, count)
            print("SendOp: sending {msg}".format(msg=msg))
            write_stream.send(msg)

            if count % 3 == 2:
                print("SendOp: sending watermark")
                write_stream.send(erdos.WatermarkMessage(timestamp))

            count += 1
            time.sleep(1)


class TopOp(Source):
    def __init__(self):
        print("initializing top op")

    def run(self, write_stream: WriteStream):
        print("TopOp: sending watermark")
        top_timestamp = erdos.Timestamp(is_top=True)
        write_stream.send(erdos.WatermarkMessage(top_timestamp))


class BatchOp(OneInOneOut):
    def __init__(self):
        print("initializing batch op")
        self.batch = []

    def on_data(self, context: OneInOneOutContext, data: Any):
        print("adding to batch: {data}".format(data=data))
        self.batch.append(data)

    def on_watermark(self, context: OneInOneOutContext):
        msg = erdos.Message(context.timestamp, self.batch)
        print("BatchOp: sending batch {msg}".format(msg=msg))
        context.write_stream.send(msg)
        self.batch = []


class CallbackWatermarkListener(Sink):
    def __init__(self):
        print("initializing callback listener op")

    def on_data(self, context: SinkContext, data: Any):
        print("CallbackWatermarkListener: received message {data}".format(data=data))

    def on_watermark(self, context: SinkContext):
        print(
            "CallbackWatermarkListener: received watermark at {}".format(
                context.timestamp
            )
        )


class PullWatermarkListener(Sink):
    def __init__(self):
        print("initializing pull listener op")

    def run(self, read_stream: ReadStream):
        while True:
            data = read_stream.read()
            if isinstance(data, erdos.WatermarkMessage):
                print(
                    ("PullWatermarkListener:" "received watermark {timestamp}").format(
                        timestamp=data.timestamp
                    )
                )
            else:
                print(
                    "PullWatermarkListener: received message {data}".format(data=data)
                )


def main():
    """Creates and runs the dataflow graph."""
    count_stream = erdos.connect_source(SendOp, erdos.operator.OperatorConfig())
    top_stream = erdos.connect_source(TopOp, erdos.operator.OperatorConfig())
    batch_stream = erdos.connect_one_in_one_out(
        BatchOp, erdos.operator.OperatorConfig(), count_stream
    )
    erdos.connect_sink(
        CallbackWatermarkListener, erdos.operator.OperatorConfig(), batch_stream
    )
    erdos.connect_sink(
        CallbackWatermarkListener, erdos.operator.OperatorConfig(), top_stream
    )
    erdos.connect_sink(
        PullWatermarkListener, erdos.operator.OperatorConfig(), batch_stream
    )

    erdos.run()


if __name__ == "__main__":
    main()
