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
MAX_MSG_COUNT = 5


class FirstOp(Op):
    def __init__(self, name):
        super(FirstOp, self).__init__(name)
        self._send_cnt = 0
        self.counts = {}

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(FirstOp.on_msg)
        return [DataStream(data_type=String, name='first_out')]

    def publish_msg(self):
        data = 'data %d' % self._send_cnt
        self._send_cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self.get_output_stream('first_out').send(output_msg)
        print('%s sent %s' % (self.name, data))

    def on_msg(self, msg):
        data = msg if type(msg) is str else msg.data
        received_cnt = int(data.split(" ")[1])
        if received_cnt not in self.counts:
            self.counts[received_cnt] = 1
        else:
            self.counts[received_cnt] += 1
        assert received_cnt < MAX_MSG_COUNT and self.counts[received_cnt] <= 1, \
            'received count %d should be smaller than total maximum received count %d, ' \
            'and can not appear more than once' \
            % (received_cnt, MAX_MSG_COUNT)
        print('%s received %s' % (self.name, msg))

    def execute(self):
        for _ in range(0, MAX_MSG_COUNT):
            self.publish_msg()
            time.sleep(1)
        time.sleep(1)


class SecondOp(Op):
    def __init__(self, name, spin=True):
        super(SecondOp, self).__init__(name)
        self._cnt = 0
        self.counts = {}
        self.is_spin = spin

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SecondOp.on_msg)
        return [DataStream(data_type=String, name='second_out')]

    def on_msg(self, msg):
        data = msg if type(msg) is str else msg.data
        received_cnt = int(data.split(" ")[1])
        if received_cnt not in self.counts:
            self.counts[received_cnt] = 1
        else:
            self.counts[received_cnt] += 1
        assert received_cnt < MAX_MSG_COUNT and self.counts[received_cnt] <= 1, \
            'received count %d should be smaller than total maximum received count %d, ' \
            'and can not appear more than once' \
            % (received_cnt, MAX_MSG_COUNT)
        self._cnt += 1
        self.get_output_stream('second_out').send(msg)
        print('%s received and sent %s' % (self.name, msg))

    def execute(self):
        if self.is_spin:
            while self._cnt < MAX_MSG_COUNT:
                time.sleep(0.1)
            time.sleep(1)


def run_graph(spin):
    graph = erdos.graph.get_current_graph()
    first = graph.add(FirstOp, name='first')
    second = graph.add(SecondOp, name='second', init_args={'spin': spin})
    graph.connect([first], [second])
    graph.connect([second], [first])
    graph.execute(FLAGS.framework)


def main(argv):
    spin = True
    if FLAGS.framework == 'ray':
        spin = False
    proc = Process(target=run_graph, args=(spin, ))
    proc.start()
    time.sleep(10)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
