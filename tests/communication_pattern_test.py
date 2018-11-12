import os
import sys
import time
from absl import app
from absl import flags
from multiprocessing import Process

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
MAX_MSG_COUNT = 5


class PublisherOp(Op):
    def __init__(self, name):
        super(PublisherOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='pub_out')]

    def publish_msg(self):
        data = 'data %d' % self._cnt
        self._cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self.get_output_stream('pub_out').send(output_msg)
        print('%s sent %s' % (self.name, data))

    def execute(self):
        for _ in range(0, MAX_MSG_COUNT):
            self.publish_msg()
            time.sleep(1)


class SubscriberOp(Op):
    def __init__(self, name, num_pub_ops=1, spin=True):
        super(SubscriberOp, self).__init__(name)
        self._cnt = 0
        self._num_pub_ops = num_pub_ops
        self.counts = {}
        self.is_spin = spin

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SubscriberOp.on_msg)
        return [DataStream(data_type=String, name='sub_out')]

    def on_msg(self, msg):
        data = msg if type(msg) is str else msg.data
        received_cnt = int(data.split(" ")[1])

        if received_cnt not in self.counts:
            self.counts[received_cnt] = 1
        else:
            self.counts[received_cnt] += 1
        max_total_count = MAX_MSG_COUNT * self._num_pub_ops
        assert received_cnt < max_total_count and self.counts[received_cnt] <= self._num_pub_ops, \
            'received count %d should be smaller than total maximum received count %d, ' \
            'and should not appear more than %d times' \
            % (received_cnt, max_total_count, self._num_pub_ops)

        self._cnt += 1
        print('%s received %s' % (self.name, msg))

    def execute(self):
        if self.is_spin:
            while self._cnt < self._num_pub_ops * MAX_MSG_COUNT:
                time.sleep(0.1)


def run_graph(n_pub, n_sub, spin):

    graph = erdos.graph.get_current_graph()

    # Add publishers
    publishers = []
    for i in range(n_pub):
        pub = graph.add(PublisherOp, name='publisher_%d' % i)
        publishers.append(pub)

    # Add subscribers
    subscribers = []
    for i in range(n_sub):
        sub = graph.add(SubscriberOp, name='subscriber_%d' % i, init_args={'num_pub_ops': n_pub, 'spin': spin})
        subscribers.append(sub)

    # Connect operators
    graph.connect(publishers, subscribers)

    # Execute graph
    graph.execute(FLAGS.framework)


def main(argv):
    spin = True
    if FLAGS.framework == 'ray':
        spin = False
    arg = FLAGS.case.split('-')
    n_pub = int(arg[0])
    n_sub = int(arg[1])
    proc = Process(target=run_graph, args=(n_pub, n_sub, spin))
    proc.start()
    time.sleep(10)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
