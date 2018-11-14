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
flags.DEFINE_string('case', '1-1',
                    '{num_publisher}-{num_subscriber}, e.g. 1-1, 2-1, 1-2')
MAX_MSG_COUNT = 5


class PublisherOp(Op):
    def __init__(self, name):
        super(PublisherOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='pub_out')]

    @deadline(100)
    def publish_msg(self):
        time.sleep(1)

    def execute(self):
        for _ in range(0, MAX_MSG_COUNT):
            self.publish_msg()
            time.sleep(1)

    def on_next_deadline_miss(self):
        print('Miss deadline!!')


class SubscriberOp(Op):
    def __init__(self, name, spin=True):
        super(SubscriberOp, self).__init__(name)
        self._cnt = 0
        self.is_spin = spin

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SubscriberOp.on_msg)
        return [DataStream(data_type=String, name='sub_out')]

    def on_msg(self, msg):
        msg = msg if type(msg) is str else msg.data
        self._cnt += 1
        print('%s received %s' % (self.name, msg))

    def execute(self):
        if self.is_spin:
            while self._cnt < MAX_MSG_COUNT:
                time.sleep(0.1)


def run_graph(spin):
    graph = erdos.graph.get_current_graph()
    pub = graph.add(PublisherOp, name='publisher')
    graph.execute(FLAGS.framework)


def main(argv):
    spin = True
    if FLAGS.framework == 'ray':
        spin = False
    proc = Process(target=run_graph, args=(spin))
    proc.start()
    time.sleep(10)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
