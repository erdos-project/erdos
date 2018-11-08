import os
import sys
from absl import app
from absl import flags

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import erdos.graph
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency

try:
    from std_msgs.msg import String
except ModuleNotFoundError:
    # ROS not installed
    String = str


FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


class FirstOp(Op):
    def __init__(self, name):
        super(FirstOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(FirstOp.on_msg)
        return [DataStream(data_type=String, name='first_out')]

    @frequency(1)
    def publish_msg(self):
        data = 'data %d' % self._cnt
        self._cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self.send('first_out', output_msg)
        print('%s sent %s' % (self.name, data))

    def on_msg(self, msg):
        print('%s received %s' % (self.name, msg))

    def execute(self):
        self.publish_msg()
        self.spin()


class SecondOp(Op):
    def __init__(self, name):
        super(SecondOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SecondOp.on_msg)
        return [DataStream(data_type=String, name='second_out')]

    def on_msg(self, msg):
        self.send('second_out', msg)
        print('%s received %s' % (self.name, msg))

    def execute(self):
        self.spin()


def main(argv):

    # Set up graph
    graph = erdos.graph.get_current_graph()

    # Add operators
    first = graph.add(FirstOp, name='first')
    second = graph.add(SecondOp, name='second')

    # Connect operators
    graph.connect([first], [second])
    graph.connect([second], [first])

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
