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
        self.is_spin = spin

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SubscriberOp.on_msg)
        return [DataStream(data_type=String, name='sub_out')]

    def on_msg(self, msg):
        data = msg if type(msg) is str else msg.data
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self._cnt += 1
        self.get_output_stream('sub_out').send(output_msg)
        print('%s received %s' % (self.name, msg))

    def execute(self):
        if self.is_spin:
            while self._cnt < MAX_MSG_COUNT:
                time.sleep(0.1)


def run_graph(spin):

    graph = erdos.graph.get_current_graph()

    # Log output of source op and input of sink op
    source = graph.add(PublisherOp, name='source', log_output_streams=True)
    internal = graph.add(SubscriberOp, name='internal',
                         init_args={'spin': spin})
    sink = graph.add(SubscriberOp, name='sink',
                     init_args={'spin': spin},
                     log_input_streams=True)

    graph.connect([source], [internal])
    graph.connect([internal], [sink])
    graph.execute(FLAGS.framework)

    # Check correctness.
    log_filename1 = 'default_source_pub_out.log'
    log_filename2 = 'default_internal_sub_out.log'
    assert os.path.isfile(log_filename1), '{} does not exist.'.format(log_filename1)
    assert os.path.isfile(log_filename2), '{} does not exist'.format(log_filename2)
    line_count1 = sum(1 for _ in open(log_filename1))
    line_count2 = sum(1 for _ in open(log_filename2))
    assert line_count1 == 5, '{} line number is {} not {}'.format(log_filename1, str(line_count1), '5')
    assert line_count2 == 5, '{} line number is {} not {}'.format(log_filename2, str(line_count2), '5')


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
