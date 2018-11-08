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
flags.DEFINE_string('case', '1-1', '{num_publisher}-{num_subscriber}, e.g. 1-1, 2-1, 1-2')


class PublisherOp(Op):
    def __init__(self, name):
        super(PublisherOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='pub_out')]

    @frequency(1)
    def publish_msg(self):
        data = 'data %d' % self._cnt
        self._cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self.send('pub_out', output_msg)
        print('%s sent %s' % (self.name, data))

    def execute(self):
        self.publish_msg()
        self.spin()


class SubscriberOp(Op):
    def __init__(self, name):
        super(SubscriberOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SubscriberOp.on_msg)
        return [DataStream(data_type=String, name='sub_out')]

    def on_msg(self, msg):
        self.send('sub_out', msg)
        print('%s received %s' % (self.name, msg))

    def execute(self):
        self.spin()


def main(argv):

    # Parse args
    arg = FLAGS.case.split('-')
    n_pub = int(arg[0])
    n_sub = int(arg[1])

    # Set up graph
    graph = erdos.graph.get_current_graph()

    # Add publishers
    publishers = []
    for i in range(n_pub):
        pub = graph.add(PublisherOp, name='publisher_%d' % i)
        publishers.append(pub)

    # Add subscribers
    subscribers = []
    for i in range(n_sub):
        sub = graph.add(SubscriberOp, name='subscriber_%d' % i)
        subscribers.append(sub)

    # Connect operators
    graph.connect(publishers, subscribers)

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
