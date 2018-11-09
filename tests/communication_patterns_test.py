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
MAXIMUM_COUNT = 5


class PublisherOp(Op):
    def __init__(self, name):
        super(PublisherOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='pub_out')]

    @frequency(1)
    def publish_msg(self):
        if self._cnt == MAXIMUM_COUNT:
            sys.exit()
        data = 'data %d' % self._cnt
        self._cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self.get_output_stream('pub_out').send(output_msg)
        print('%s sent %s' % (self.name, data))

    def execute(self):
        self.publish_msg()
        self.spin()


class SubscriberOp(Op):
    def __init__(self, name):
        super(SubscriberOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SubscriberOp.on_msg)
        return [DataStream(data_type=String, name='sub_out')]

    def on_msg(self, msg):
        count = int(msg.split(" ")[1])
        assert count < MAXIMUM_COUNT and count == self._cnt
        self._cnt += 1
        print('%s received %s' % (self.name, msg))

    def execute(self):
        self.spin()


def run_graph():
    graph = erdos.graph.get_current_graph()
    pub = graph.add(PublisherOp, name='publisher_%d' % i)
    sub = graph.add(SubscriberOp, name='subscriber_%d' % i)
    graph.connect([pub], [sub])
    graph.execute(FLAGS.framework)


def main(argv):
    proc = Process(target=run_graph)
    proc.start()
    time.time(10)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
