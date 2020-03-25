"""Merges messages from two senders based on message number (count).
A MeasurementOp finds the difference between the max system time and the min system time
of the two timestamps in a joined message.
If logical time is used for timestamps, the difference is in seconds.
If system time is used for timestamps, the difference is in microseconds.
"""

import time

from erdos import utils
import erdos


class SendOp(erdos.Operator):
    """Sends `frequency` messages per second."""
    def __init__(self, write_stream, frequency, logical_bool):
        self.frequency = frequency
        self.write_stream = write_stream
        self.logical_bool = logical_bool

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = 0
        while True:
            if self.logical_bool:
                timestamp = erdos.Timestamp(coordinates=[count])
                msg = erdos.Message(timestamp, (time.time(), count))
            else:
                timestamp = erdos.Timestamp(
                    coordinates=[round(time.time() * 1e6)])
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
    def __init__(self, left_stream, right_stream, write_stream, logical_bool):
        self.left_msgs = []
        self.right_msgs = []
        self.logical_bool = logical_bool
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
        self.left_msgs.append(msg)

    def recv_right(self, msg):
        print("JoinOp: received {msg} on right stream".format(msg=msg))
        self.right_msgs.append(msg)

    # TODO: use a callback on a stateful read stream instead of passing self
    def send_joined(self, timestamp, write_stream):
        if len(self.left_msgs) == 0 or len(self.right_msgs) == 0:
            return
        left_msg = self.left_msgs.pop(0)
        right_msg = self.right_msgs.pop(0)
        if self.logical_bool:
            joined_msg = erdos.Message(timestamp,
                                       (left_msg.data, right_msg.data))
        else:
            joined_msg = erdos.Message(timestamp,
                                       ((left_msg.timestamp, left_msg.data),
                                        (right_msg.timestamp, right_msg.data)))
        print("JoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
        write_stream.send(joined_msg)


class MeasurementOp(erdos.Operator):
    def __init__(self, read_stream, write_stream, logical_bool):
        read_stream.add_callback(self.callback, [write_stream])
        self.logical_bool = logical_bool
        self.logger = utils.setup_csv_logging("experiment",
                                              log_file="JasmineExperiment")

    def callback(self, msg, write_stream):
        print("MeasurementOp: receiving {msg}".format(msg=msg))
        left_data, right_data = msg.data[0], msg.data[1]
        if self.logical_bool:
            left_time, right_time = left_data[0], right_data[0]
        else:
            left_time, right_time = left_data[0].coordinates[0], right_data[
                0].coordinates[0]
        min_time, max_time = min(left_time,
                                 right_time), max(left_time, right_time)
        difference = max_time - min_time
        msg = erdos.Message(msg.timestamp, difference)

        write_stream.send(msg)
        self.logger.warning(
            "{timestamp}, {min_age}, {max_age}, {difference}".format(
                timestamp=msg.timestamp,
                min_age=min_time,
                max_age=max_time,
                difference=difference))
        print("MeasurementOp: sending {msg}".format(msg=msg))

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]


def main():
    """Creates and runs the dataflow graph."""
    f_1, f_2, logical_bool = 1, 2, False

    (left_stream, ) = erdos.connect(SendOp,
                                    erdos.OperatorConfig(name="F_1SendOp"), [],
                                    frequency=f_1,
                                    logical_bool=logical_bool)
    (right_stream, ) = erdos.connect(SendOp,
                                     erdos.OperatorConfig(name="F_2SendOp"),
                                     [],
                                     frequency=f_2,
                                     logical_bool=logical_bool)
    (join_stream, ) = erdos.connect(JoinOp,
                                    erdos.OperatorConfig(),
                                    [left_stream, right_stream],
                                    logical_bool=logical_bool)
    (time_stream, ) = erdos.connect(MeasurementOp,
                                    erdos.OperatorConfig(), [join_stream],
                                    logical_bool=logical_bool)
    erdos.run()


if __name__ == "__main__":
    main()
