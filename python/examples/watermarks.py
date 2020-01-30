"""Every second, sends the message count to the batch operator.
Sends a watermark every 3 messages which releases the batch.
"""

import erdos
import sys
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
            timestamp = erdos.Timestamp(coordinates=[count])
            msg = erdos.Message(timestamp, count)
            print("SendOp: sending {msg}".format(msg=msg))
            self.write_stream.send(msg)

            if count % 3 == 2:
                print("sendOp: sending watermark")
                self.write_stream.send(erdos.WatermarkMessage(timestamp))

            count += 1
            time.sleep(1)


class TopOp(erdos.Operator):
    def __init__(self, write_stream):
        self.write_stream = write_stream

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        top_timestamp = erdos.Timestamp(coordinates=[sys.maxsize])
        self.write_stream.send(erdos.WatermarkMessage(top_timestamp))


class BatchOp(erdos.Operator):
    def __init__(self, read_stream, write_stream):
        read_stream.add_callback(self.add_to_batch)
        read_stream.add_watermark_callback(self.send_batch, [write_stream])
        self.batch = []

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

    # TODO: use a callback on a stateful read stream instead of passing self
    def add_to_batch(self, msg):
        print("adding to batch: {msg}".format(msg=msg))
        self.batch.append(msg.data)

    # TODO: use a callback on a stateful read stream instead of passing self
    def send_batch(self, timestamp, write_stream):
        msg = erdos.Message(timestamp, self.batch)
        print("BatchOp: sending batch {msg}".format(msg=msg))
        write_stream.send(msg)
        self.batch = []


class CallbackWatermarkListener(erdos.Operator):
    def __init__(self, read_stream, top_stream):
        read_stream.add_callback(lambda m: print(
            "CallbackWatermarkListener: received message {m}".format(m=m)))
        erdos.add_watermark_callback(
            [read_stream, top_stream], [],
            lambda t: print("CallbackWatermarkListener: received watermark {t}"
                            .format(t=t)))

    @staticmethod
    def connect(read_stream, top_stream):
        return []


class PullWatermarkListener(erdos.Operator):
    def __init__(self, read_stream):
        self.read_stream = read_stream

    @staticmethod
    def connect(*read_streams):
        return []

    def run(self):
        while True:
            msg = self.read_stream.read()
            if isinstance(msg, erdos.WatermarkMessage):
                print(("PullWatermarkListener:"
                       "received watermark {timestamp}").format(
                           timestamp=msg.timestamp))
            else:
                print("PullWatermarkListener: received message {msg}".format(
                    msg=msg))


def driver():
    """Creates the dataflow graph."""
    (count_stream, ) = erdos.connect(SendOp, [])
    (top_stream, ) = erdos.connect(TopOp, [])
    (batch_stream, ) = erdos.connect(BatchOp, [count_stream],
                                     flow_watermarks=True)
    erdos.connect(CallbackWatermarkListener, [batch_stream, top_stream])
    erdos.connect(PullWatermarkListener, [batch_stream])


if __name__ == "__main__":
    erdos.run(driver)
