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
from erdos.utils import deadline

try:
    from std_msgs.msg import String
except ModuleNotFoundError:
    # ROS not installed
    String = str

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
MAX_MSG_COUNT = 50


class PublisherOp(Op):
    def __init__(self, name):
        super(PublisherOp, self).__init__(name)
        self.idx = 0

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='pub_out')]

    @deadline(10, "on_next_deadline_miss")
    def publish_msg(self):
        if self.idx % 2 == 0:
            time.sleep(0.02)
        data = 'data %d' % self.idx
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self.get_output_stream('pub_out').send(output_msg)
        self.idx += 1

    def execute(self):
        for _ in range(0, MAX_MSG_COUNT):
            self.publish_msg()

    def on_next_deadline_miss(self):
        assert self.idx % 2 == 0
        print('%s missed deadline on data %d' % (self.name, self.idx))


class SubscriberOp(Op):
    def __init__(self, name, spin=True):
        super(SubscriberOp, self).__init__(name)
        self.is_spin = spin
        self.idx = 0

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SubscriberOp.on_msg)
        return [DataStream(data_type=String, name='sub_out')]

    @deadline(10, "on_next_deadline_miss")
    def on_msg(self, msg):
        if self.idx % 2 == 0:
            time.sleep(0.02)
        self.idx += 1

    def execute(self):
        if self.is_spin:
            while self.idx < MAX_MSG_COUNT:
                time.sleep(0.1)

    def on_next_deadline_miss(self):
        assert self.idx % 2 == 0
        print('%s missed deadline on data %d' % (self.name, self.idx))


def run_graph(spin):
    graph = erdos.graph.get_current_graph()
    pub = graph.add(PublisherOp, name='publisher')
    sub = graph.add(SubscriberOp, name='subscriber', init_args={'spin': spin})
    graph.connect([pub], [sub])
    graph.execute(FLAGS.framework)


def main(argv):
    spin = True
    if FLAGS.framework == 'ray':
        spin = False
    proc = Process(target=run_graph, args=(spin,))
    proc.start()
    time.sleep(5)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
