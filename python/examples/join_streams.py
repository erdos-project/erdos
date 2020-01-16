"""Merges messages from two senders based on timestamp.
"""

import erdos
import time


class SendOp(erdos.Operator):
    """Sends `frequency` messages per second."""
    def __init__(self, write_stream, name, frequency):
        self.name = name
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
            print(f"{self.name}: sending {msg}")
            self.write_stream.send(msg)

            watermark = erdos.WatermarkMessage(timestamp)
            print(f"{self.name}: sending watermark {watermark}")
            self.write_stream.send(watermark)

            count += 1
            time.sleep(1 / self.frequency)


class JoinOp(erdos.Operator):
    def __init__(self, left_stream, right_stream, write_stream):
        self.left_msgs = {}
        self.right_msgs = {}
        left_stream.add_callback(self.recv_left)
        right_stream.add_callback(self.recv_right)
        erdos.add_watermark_callback([left_stream, right_stream],
                                      [write_stream], self.send_joined)

    @staticmethod
    def connect(left_stream, right_stream):
        return [erdos.WriteStream()]

    # TODO: use a callback on a stateful read stream instead of passing self
    def recv_left(self, msg):
        print(f"JoinOp: received {msg} on left stream")
        self.left_msgs[msg.timestamp] = msg

    def recv_right(self, msg):
        print(f"JoinOp: received {msg} on right stream")
        self.right_msgs[msg.timestamp] = msg

    # TODO: use a callback on a stateful read stream instead of passing self
    def send_joined(self, timestamp, write_stream):
        left_msg = self.left_msgs.pop(timestamp)
        right_msg = self.right_msgs.pop(timestamp)
        joined_msg = erdos.Message(timestamp, (left_msg.data, right_msg.data))
        print(f"JoinOp: sending {joined_msg}")
        write_stream.send(joined_msg)


def driver():
    """Creates the dataflow graph."""
    (left_stream, ) = erdos.connect(
        SendOp, [], True, "FastSendOp", frequency=2)
    (right_stream, ) = erdos.connect(
        SendOp, [], True, "SlowSendOp", frequency=1)
    erdos.connect(JoinOp, [left_stream, right_stream])


if __name__ == "__main__":
    erdos.run(driver)
