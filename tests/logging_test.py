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

try:
    from std_msgs.msg import String
except ModuleNotFoundError:
    # ROS not installed
    String = str

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
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
    def __init__(self, name, spin=True):
        super(SubscriberOp, self).__init__(name)
        self._cnt = 0
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

        self._cnt += 1
        print('%s received %s' % (self.name, msg))

    def execute(self):
        if self.is_spin:
            while self._cnt < MAX_MSG_COUNT:
                time.sleep(0.1)


def run_graph(spin):

    graph = erdos.graph.get_current_graph()
    pub = graph.add(PublisherOp, name='publisher', log_output_streams=True)
    sub = graph.add(
        SubscriberOp,
        name='subscriber',
        init_args={'spin': spin},
        log_input_streams=True
        )
    graph.connect([pub], [sub])
    graph.execute(FLAGS.framework)


def main(argv):
    spin = True
    if FLAGS.framework == 'ray':
        spin = False
    proc = Process(target=run_graph, args=(spin,))
    proc.start()
    time.sleep(10)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
