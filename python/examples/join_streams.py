"""Merges messages from two senders based on timestamp.
"""

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

            watermark = erdos.WatermarkMessage(timestamp)
            print("{name}: sending watermark {watermark}".format(
                name=self.config.name, watermark=watermark))
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
        print("JoinOp: received {msg} on left stream".format(msg=msg))
        self.left_msgs[msg.timestamp] = msg

    def recv_right(self, msg):
        print("JoinOp: received {msg} on right stream".format(msg=msg))
        self.right_msgs[msg.timestamp] = msg

    # TODO: use a callback on a stateful read stream instead of passing self
    def send_joined(self, timestamp, write_stream):
        left_msg = self.left_msgs.pop(timestamp)
        right_msg = self.right_msgs.pop(timestamp)
        joined_msg = erdos.Message(timestamp, (left_msg.data, right_msg.data))
        print("JoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
        write_stream.send(joined_msg)


def main():
    """Creates and runs the dataflow graph."""
    (left_stream, ) = erdos.connect(SendOp,
                                    erdos.OperatorConfig(name="FastSendOp"),
                                    [],
                                    frequency=2)
    (right_stream, ) = erdos.connect(SendOp,
                                     erdos.OperatorConfig(name="SlowSendOp"),
                                     [],
                                     frequency=1)
    erdos.connect(JoinOp, erdos.OperatorConfig(), [left_stream, right_stream])

    erdos.run()


if __name__ == "__main__":
    main()
