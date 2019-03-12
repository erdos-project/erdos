from collections import defaultdict
from time import sleep

from absl import app
from absl import flags

import erdos.graph
from erdos.op import Op
from erdos.utils import frequency
from erdos.message import Message
from erdos.data_stream import DataStream
from erdos.timestamp import Timestamp

INTEGER_FREQUENCY = 10 # The frequency at which to send the integers.

class FirstOperator(Op):
    """ Source operator that publishes increasing integers at a fixed frequency.
    The operator also inserts a watermark after a fixed number of messages.
    """

    def __init__(self, name, batch_size):
        """ Initializes the attributes to be used by the source operator."""
        super(FirstOperator, self).__init__(name)
        self.batch_size = batch_size
        self.counter = 1
        self.batch_number = 1

    @staticmethod
    def setup_streams(input_streams):
        """ Outputs a single stream where the messages are sent. """
        return [DataStream(data_type = int, name = "integer_out")]

    @frequency(INTEGER_FREQUENCY)
    def publish_numbers(self):
        """ Sends an increasing count of numbers 
            to the downstream operators. """
        output_msg = Message(self.counter, 
                             Timestamp(coordinates = [self.batch_number]))
        self.get_output_stream("integer_out").send(output_msg)
       
        # Decide if the watermark needs to be sent.
        if self.counter % self.batch_size == 0:
            # The batch has completed. We need to send a watermark now.
            watermark_msg = Message(self.batch_number, 
                               Timestamp(coordinates = [self.batch_number]),
                               watermark = True) 
            self.batch_number += 1
            self.get_output_stream("integer_out").send(watermark_msg)

        # Update the counters.
        self.counter += 1

    def execute(self):
        """ Execute the publish number loop. """
        self.publish_numbers()
        self.spin() 

class SecondOperator(Op):
    """ Second operator that listens in on the numbers and reports their
    sum when the watermark is received. """
    
    def __init__(self, name):
        """ Initializes the attributes to be used."""
        super(SecondOperator, self).__init__(name)
        self.windows = defaultdict(list)

    @staticmethod
    def setup_streams(input_streams):
        """ Subscribes all the input streams to the save numbers callback. """
        input_streams.add_callback(SecondOperator.save_numbers)
        input_streams.add_completion_callback(SecondOperator.execute_sum)
        return [DataStream(data_type = int, name = "sum_out")]

    def save_numbers(self, message): 
        """ Save all the numbers corresponding to a window. """
        batch_number = message.timestamp.coordinates[0]
        self.windows[batch_number].append(message.data)

    def execute_sum(self, message):
        """ Sum all the numbers in this window and send out the aggregate. """
        batch_number = message.timestamp.coordinates[0]
        window_data = self.windows.pop(batch_number, None) 
        #print("Received a watermark for the timestamp: {}".format(batch_number))
        #print("The sum of the window {} is {}".format(
        #              window_data, sum(window_data))) 
        output_msg = Message(sum(window_data),
                      Timestamp(coordinates = [batch_number]))
        self.get_output_stream("sum_out").send(output_msg)

    def execute(self):
        """ Execute the spin() loop to continue processing messages. """ 
        self.spin()

class ThirdOperator(Op):
    """ Third operator that listens in on the sum and verifies correctness."""
    
    def __init__(self, name):
        """Initializes the attributes to be used."""
        super(ThirdOperator, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        """ Subscribes all the input streams to the assert callback."""
        input_streams.add_callback(ThirdOperator.assert_correctness)
        return []
 
    def assert_correctness(self, message):
        """ Assert the correctness of the results."""
        batch_number = message.timestamp.coordinates[0]
        sum_data = sum(range((batch_number - 1) * 10 + 1, batch_number * 10 + 1)) 
        print("Received sum: {} for the batch_number {}, expected {}".format(
                      message.data, batch_number, sum_data))

def main(argv):
    # Set up the graph.
    graph = erdos.graph.get_current_graph()

    # Add the operators.
    source_op = graph.add(FirstOperator, name = "gen_op", init_args = {'batch_size' : 10})
    sum_op    = graph.add(SecondOperator, name = "sum_op")
    assert_op = graph.add(ThirdOperator, name = "assert_op")

    # Connect the operators.
    graph.connect([source_op], [sum_op])
    graph.connect([sum_op], [assert_op])

    # Execute the graph.
    graph.execute('ray')

if __name__ == "__main__":
    app.run(main)
