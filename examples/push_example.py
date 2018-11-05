import os
import sys
from absl import app
from absl import flags

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import erdos.graph
from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.timestamp import Timestamp
from erdos.utils import frequency

from std_msgs.msg import String

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


class InputOperator(Op):
    def __init__(self, name):
        super(InputOperator, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='input_stream')]

    @frequency(1)
    def publish(self):
        data = 'hello world %d' % self._cnt
        self._cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('input_stream').send(output_msg)
        print('%s sent %s' % (self.name, data))

    def execute(self):
        self.publish()


class PushOperator(Op):
    def __init__(self, name='push_op'):
        super(PushOperator, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(PushOperator.on_next)
        return [DataStream(name='push_op_output', data_type=String)]

    def on_next(self, data):
        print('%s received %s' % (self.name, data))

    def execute(self):
        self.spin()


def main(argv):
    # Set up graph
    graph = erdos.graph.get_current_graph()

    # Add operators
    input_op = graph.add(InputOperator, name='input_op')
    push_op = graph.add(PushOperator, name='push_op')

    # Connect graph
    graph.connect([input_op], [push_op])

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
