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
MAXIMUM_COUNT = 5


class FirstOp(Op):
    def __init__(self, name):
        super(FirstOp, self).__init__(name)
        self._cnt = 0
        self._receive_cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(FirstOp.on_msg)
        return [DataStream(data_type=String, name='first_out')]

    @frequency(1)
    def publish_msg(self):
        if self._cnt == MAXIMUM_COUNT:
            sys.exit()
        data = 'data %d' % self._cnt
        self._cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[0]))
        self.get_output_stream('first_out').send(output_msg)
        print('%s sent %s' % (self.name, data))

    def on_msg(self, msg):
        if type(msg) is not str:
            data = msg.data
        count = int(data.split(" ")[1])
        assert count < MAXIMUM_COUNT and count == self._receive_cnt, \
            'received count %d should be equal to %d and smaller than maximum count %d' \
            % (count, self._receive_cnt, MAXIMUM_COUNT)
        self._receive_cnt += 1
        print('%s received %s' % (self.name, msg))

    def execute(self):
        self.publish_msg()
        self.spin()


class SecondOp(Op):
    def __init__(self, name):
        super(SecondOp, self).__init__(name)
        self._receive_cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SecondOp.on_msg)
        return [DataStream(data_type=String, name='second_out')]

    def on_msg(self, msg):
        if type(msg) is not str:
            data = msg.data
        count = int(data.split(" ")[1])
        assert count < MAXIMUM_COUNT and count == self._receive_cnt, \
            'received count %d should be equal to %d and smaller than maximum count %d' \
            % (count, self._receive_cnt, MAXIMUM_COUNT)
        self._receive_cnt += 1
        self.get_output_stream('second_out').send(msg)
        print('%s received %s' % (self.name, msg))

    def execute(self):
        self.spin()


def run_graph():
    graph = erdos.graph.get_current_graph()
    first = graph.add(FirstOp, name='first')
    second = graph.add(SecondOp, name='second')
    graph.connect([first], [second])
    graph.connect([second], [first])
    graph.execute(FLAGS.framework)


def main(argv):
    proc = Process(target=run_graph)
    proc.start()
    time.sleep(10)
    proc.terminate()


if __name__ == '__main__':
    app.run(main)
