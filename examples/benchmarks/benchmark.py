import time

from absl import app
from absl import flags
from std_msgs.msg import Int64
from std_msgs.msg import String
import numpy as np

from erdos.graph import Graph
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency

# import ROS autogenerate msg file
from Image import *

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_bool('periodic_jitter', False, 'Benchmark periodic task jitter.')
flags.DEFINE_bool('aperiodic_latency', False,
                  'Benchmark aperiodic task spawning latency.')
flags.DEFINE_bool('throughput', False, 'Benchmark data stream throughput.')
flags.DEFINE_integer('periodic_jitter_num_msg', 10000,
                     'Number of periodic msgs to send.')
flags.DEFINE_bool('throughput_loop', False, 'Benchmrk ping pong loop.')
flags.DEFINE_integer('num_iterations', 10000,
                     'Number of iterations to complete.')


class DataIntGeneratorOp(Op):
    def __init__(self, name, num_msgs):
        super(DataIntGeneratorOp, self).__init__(name)
        self._num_msgs = num_msgs
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, setup_args):
        return [DataStream(data_type=Int64, name='data_stream')]

    @frequency(1000)
    def publish_msg(self):
        self._cnt += 1
        # XXX(ionel): Hack! We send more messages because the first several
        # messages are lost in ROS because subscribers may start after
        # publishers already publish several messages.
        if self._cnt <= self._num_msgs + 10000:
            output_msg = Message(self._cnt, Timestamp(coordinates=[self._cnt]))
            self.get_output_streams('data_stream').send(output_msg)

    def execute(self):
        self.publish_msg()
        self.spin()


class SinkIntOp(Op):
    def __init__(self, name, num_msgs):
        super(SinkIntOp, self).__init__(name)
        self._num_msgs = num_msgs
        self._cnt = 0
        self._last_time = None
        self._values = []

    @staticmethod
    def setup_streams(input_streams, setup_args):
        input_streams.add_callback(SinkIntOp.on_msg)
        return []

    def on_msg(self, msg):
        if self._last_time is None:
            self._last_time = time.time()
        else:
            cur_time = time.time()
            diff_ms = (cur_time - self._last_time) * 1000
            self._last_time = cur_time
            self._values.append(diff_ms)
        self._cnt += 1
        if self._cnt % 10000 == 0:
            print('{} received {} messages'.format(time.time(), self._cnt))
        if self._cnt == self._num_msgs:
            with open('jitter.log', 'w') as jitter_file:
                for value in self._values:
                    jitter_file.write(str(value) + '\n')

    def execute(self):
        self.spin()


class ImageGeneratorOp(Op):
    def __init__(self, name):
        super(ImageGeneratorOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, setup_args):
        return [DataStream(data_type=Image, name='data_stream')]

    @frequency(150)
    def publish_msg(self, image):
        self._cnt += 1
        # XXX(ionel): This requires the ros_data_stream to use the data types.
        # I do not wrap the output into a message because that requires the
        # data to be pickled and unpickled, which adds a lot of overhead.
        self.get_output_stream('data_stream').send(image)
        # output_msg = Message(image, Timestamp(coordinates=[self._cnt]))
        #self.get_output_stream('data_stream').send(output_msg)

    def execute(self):
        img = Image()
        self.publish_msg(img)
        self.spin()


class DataGeneratorOp(Op):
    def __init__(self, name):
        super(DataGeneratorOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, setup_args):
        return [DataStream(data_type=np.array, name='data_stream')]

    @frequency(10)
    def publish_msg(self, data):
        self._cnt += 1
        output_msg = Message(data, Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('data_stream').send(output_msg)

    def execute(self):
        # XXX(ionel): The operator is slow when executed in ROS because data is
        # pickled before sending it, and unpickled upon receipt.
        data = np.zeros(13107200, dtype=int)  # 100 MB
        self.publish_msg(data)
        self.spin()


class ForwardOp(Op):
    def __init__(self, name):
        super(ForwardOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams, setup_args):
        input_streams.add_callback(ForwardOp.on_msg)
        return [DataStream(data_type=np.array, name='fwd_stream')]

    def on_msg(self, msg):
        self.get_output_stream('fwd_stream').send(msg)

    def execute(self):
        self.spin()


class SinkOp(Op):
    def __init__(self, name):
        super(SinkOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams, setup_args):
        input_streams.add_callback(SinkOp.on_msg)
        return []

    def on_msg(self, msg):
        self._cnt += 1
        if self._cnt % 150 == 0:
            print('{} received {} messages'.format(time.time(), self._cnt))

    def execute(self):
        self.spin()


class LoopIngestOp(Op):
    def __init__(self, name, num_iterations):
        super(LoopIngestOp, self).__init__(name)
        self._num_iterations = num_iterations
        self._cnt = 0
        self._start_time = None

    @staticmethod
    def setup_streams(input_streams, setup_args):
        input_streams.add_callback(LoopIngestOp.on_reply)
        return DataStream(data_type=String, name='loop_ingest')

    def trigger_loop(self):
        self._start_time = time.time()
        output_msg = Message(self._cnt, Timestamp(coordinates=[0]))
        self.get_output_stream('loop_ingest').send(output_msg)

    def on_reply(self, msg):
        self._cnt += 1
        if self._cnt == self._num_iterations:
            duration = time.time() - self._start_time
            print('Completed {} iterations in {}'.format(self._cnt, duration))
        else:
            output_msg = Message(self._cnt, Timestamp(coordinates=[0]))
            self.get_output_stream('loop_ingest').send(output_msg)

    def execute(self):
        self.trigger_loop()
        self.spin()


class LoopEgressOp(Op):
    def __init__(self, name):
        super(LoopEgressOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams, setup_args):
        input_streams.add_callback(LoopEgressOp.on_msg)
        return [DataStream(data_type=String, name='loop_egress')]

    def on_msg(self, msg):
        self.get_output_stream('loop_egress').send(msg)

    def execute(self):
        self.spin()


def main(argv):
    graph = Graph(name='benchmark')
    if FLAGS.periodic_jitter:
        generator = graph.add(
            DataIntGeneratorOp,
            name='data_generator',
            init_args={'name_msgs': FLAGS.periodic_jitter_num_msg})
        sink = graph.add(
            SinkIntOp,
            name='sink_op',
            init_args={'name_msgs': FLAGS.periodic_jitter_num_msg})
        graph.connect([generator], [sink])

    elif FLAGS.aperiodic_latency:
        pass

    elif FLAGS.throughput:
        generator = graph.add(DataIntGeneratorOp, name='data_generator')
        sink = graph.add(SinkOp, name='sink_op')
        graph.connect([generator], [sink])

    elif FLAGS.throughput_loop:
        loop_ingest = graph.add(
            LoopIngestOp,
            name='loop_ingest',
            init_args={'num_iterations': FLAGS.num_iterations})
        second_op = graph.add(LoopEgressOp, name='second_op')
        graph.connect([loop_ingest], [second_op])
        graph.connect([second_op], [loop_ingest])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
