"""Every second, sends the message count to the batch operator.
Sends a watermark every 3 messages which releases the batch.
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
            timestamp = erdos.Timestamp(coordinates=[count])
            msg = erdos.Message(timestamp, count)
            print(f"SendOp: sending {msg}")
            self.write_stream.send(msg)

            if count % 3 == 2:
                print("sendOp: sending watermark")
                self.write_stream.send(erdos.WatermarkMessage(timestamp))

            count += 1
            time.sleep(1)


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
        print(f"adding to batch: {msg}")
        self.batch.append(msg.data)

    # TODO: use a callback on a stateful read stream instead of passing self
    def send_batch(self, timestamp, write_stream):
        msg = erdos.Message(timestamp, self.batch)
        print(f"BatchOp: sending batch {msg}")
        write_stream.send(msg)
        self.batch = []


class CallbackWatermarkListener(erdos.Operator):
    def __init__(self, read_stream):
        read_stream.add_watermark_callback(lambda t: print(
            f"CallbackWatermarkListener: received watermark {t}"))
        read_stream.add_callback(lambda m: print(
            f"CallbackWatermarkListener: received message {m}"))

    @staticmethod
    def connect(*read_streams):
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
                       f"received watermark {msg.timestamp}"))
            else:
                print(f"PullWatermarkListener: received message {msg}")


def driver():
    """Creates the dataflow graph."""
    (count_stream, ) = erdos.connect(SendOp, [])
    (batch_stream, ) = erdos.connect(BatchOp, [count_stream],
                                      flow_watermarks=True)
    erdos.connect(CallbackWatermarkListener, [batch_stream])
    erdos.connect(PullWatermarkListener, [batch_stream])


if __name__ == "__main__":
    erdos.run(driver)
