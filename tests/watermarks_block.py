from absl import app
from absl import flags

from collections import defaultdict

import erdos.graph
from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.message import WatermarkMessage
from erdos.utils import frequency
from erdos.timestamp import Timestamp

INTEGER_FREQUENCY = 10


class SourceOperator(Op):
    """ SourceOperator generates the data to be fed into the SinkOperator. """

    def __init__(self, name):
        super(SourceOperator, self).__init__(name)
        self.counter = 1

    @staticmethod
    def setup_streams(input_streams):
        return [
            DataStream(data_type=int, name="output_1"),
            DataStream(data_type=int, name="output_2")
        ]

    @frequency(INTEGER_FREQUENCY)
    def publish_watermarks(self):
        """ Sends watermarks to the downstream operators. """

        # First, send the watermark on one stream.
        watermark_msg = WatermarkMessage(Timestamp(coordinates=[self.counter]))
        self.get_output_stream("output_1").send(watermark_msg)

        # Send messages on the other stream.
        message_1 = Message(self.counter,
                            Timestamp(coordinates=[self.counter]))
        message_2 = Message(self.counter + 1,
                            Timestamp(coordinates=[self.counter]))
        self.get_output_stream("output_2").send(message_1)
        self.get_output_stream("output_2").send(message_2)

        # Now, send the watermark on the second stream.
        self.get_output_stream("output_2").send(watermark_msg)

        self.counter += 1

    def execute(self):
        self.publish_watermarks()
        self.spin()


class DebugOperator(Op):
    """ DebugOperator outputs the message data to the system output. """

    def __init__(self, name):
        super(DebugOperator, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(DebugOperator.print_msg)
        return [DataStream(data_type = int, name = "debug_out")]

    def print_msg(self, msg):
        print(
            "[{}] Received the message with data {} from the stream {}".format(
                self.name, msg.data, msg.stream_name))
        self.get_output_stream("debug_out").send(msg)


class SinkOperator(Op):
    """ SinkOperator asserts that the watermark behavior is as expected. """

    def __init__(self, name):
        super(SinkOperator, self).__init__(name)
        self.timestamp_to_data_map = defaultdict(list)
        self._expected_timestamp = 1

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SinkOperator.on_msg)
        input_streams.add_completion_callback(SinkOperator.on_watermark)
        return []

    def on_msg(self, msg):
        print(
            "[{}] Received the message with data {} and timestamp {}.".format(
                self.name, msg.data, msg.timestamp.coordinates[0]))
        self.timestamp_to_data_map[msg.timestamp.coordinates[0]].append(
            msg.data)

    def on_watermark(self, msg):
        # Check we didn't miss any watermarks.
        assert self._expected_timestamp == msg.timestamp.coordinates[0]
        self._expected_timestamp += 1
        print("[{}] Received the watermark for timestamp {}.".format(
            self.name, msg.timestamp.coordinates[0]))
        data_values = self.timestamp_to_data_map[msg.timestamp.coordinates[0]]
        self.timestamp_to_data_map[msg.timestamp.coordinates[0]] = []
        print("[ASSERT] Expected value {} and got {}.".format(
            msg.timestamp.coordinates[0] + (msg.timestamp.coordinates[0] + 1),
            sum(data_values)))


def main(argv):
    # Set up the graph.
    graph = erdos.graph.get_current_graph()

    # Add the operators.
    source_op_1 = graph.add(SourceOperator, name = "FirstOperator")
    debug_op_1  = graph.add(DebugOperator, name = "DebugOperator")
    sink_op_1 = graph.add(SinkOperator, name = "SinkOperator")

    # Connect the operators.
    graph.connect([source_op_1], [debug_op_1])
    graph.connect([debug_op_1], [sink_op_1])

    # Execute the graph.
    graph.execute('ray')

if __name__ == "__main__":
    app.run(main)
