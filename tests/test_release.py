from absl import app
from absl import flags

import erdos.graph
from erdos.op import Op
from erdos.operators import EarlyReleaseOperator
from erdos.utils import frequency
from erdos.message import Message
from erdos.message import WatermarkMessage
from erdos.data_stream import DataStream
from erdos.timestamp import Timestamp

INTEGER_FREQUENCY = 10
BATCH_SIZE = 10


class SourceOperator(Op):
    """ SourceOperator generates the data to be fed into
    the ReleaseOperator."""

    def __init__(self, name, batch_size, send_batch, output_stream_name):
        super(SourceOperator, self).__init__(name)
        self.batch_size = batch_size
        self.send_batch = send_batch
        self.counter = 1
        self.batch_number = 1
        self.window = []
        self.output_stream_name = output_stream_name

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        return [DataStream(data_type=int, name=output_stream_name)]

    @frequency(INTEGER_FREQUENCY)
    def publish_numbers(self):
        """ Sends an increasing count of numbers with breaks at
        pre-defined steps."""
        output_msg = Message(self.counter,
                             Timestamp(coordinates=[self.batch_number]))
        if not self.send_batch(self.batch_number):
            # If this batch_number does not need to be sent as a batch, then
            # send the individual messages and if the batch is complete, send a
            # watermark.
            self.get_output_stream(self.output_stream_name).send(output_msg)
            if self.counter % self.batch_size == 0:
                # The batch has completed, send the watermark.
                watermark_msg = WatermarkMessage(
                    Timestamp(coordinates=[self.batch_number]))
                self.batch_number += 1
                self.get_output_stream(
                    self.output_stream_name).send(watermark_msg)
        else:
            # This batch number needs to be batched, add it to the window and
            # then check if the batch_size number of messages exist in the
            # window. If yes, send everything including the watermark message.
            self.window.append(output_msg)
            if self.counter % self.batch_size == 0:
                # The batch has completed, send everything including the
                # watermark.
                for output_msg in self.window:
                    self.get_output_stream(
                        self.output_stream_name).send(output_msg)
                watermark_msg = WatermarkMessage(
                    Timestamp(coordinates=[self.batch_number]))
                self.batch_number += 1
                self.get_output_stream(
                    self.output_stream_name).send(watermark_msg)
                self.window = []
        self.counter += 1

    def execute(self):
        self.publish_numbers()
        self.spin()


class SinkOperator(Op):
    def on_msg(self, msg):
        print("[{}] Received the message {}".format(self.name, msg.data))

    def on_completion(self, msg):
        print("[{}] Received watermark for {}".format(self.name,
                                                      msg.timestamp))

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SinkOperator.on_msg)
        input_streams.add_completion_callback(SinkOperator.on_completion)
        return []


def main(argv):
    # Set up the graph.
    graph = erdos.graph.get_current_graph()

    # Add the operators.
    source_op_1 = graph.add(
        SourceOperator,
        name="FirstOperator",
        init_args={
            'batch_size': BATCH_SIZE,
            'send_batch': lambda x: x % 2 == True,
            'output_stream_name': 'first-stream',
        },
        setup_args={
            'output_stream_name': 'first-stream',
        })
    source_op_2 = graph.add(
        SourceOperator,
        name="SecondOperator",
        init_args={
            'batch_size': BATCH_SIZE,
            'send_batch': lambda x: x % 2 == False,
            'output_stream_name': 'second-stream',
        },
        setup_args={
            'output_stream_name': 'second-stream',
        })
    release_op = graph.add(
        EarlyReleaseOperator,
        name="ReleaseOperator",
        init_args={
            'output_stream_name': 'release-stream',
        },
        setup_args={
            'output_stream_name': 'release-stream',
        })
    sink_op = graph.add(SinkOperator, name="SinkOperator")

    # Connect the operators.
    graph.connect([source_op_1, source_op_2], [release_op])
    graph.connect([release_op], [sink_op])

    # Execute the graph.
    graph.execute('ray')


if __name__ == "__main__":
    app.run(main)
