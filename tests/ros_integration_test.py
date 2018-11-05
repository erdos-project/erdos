import os
import sys
import random
from absl import app
from absl import flags

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import erdos.graph
from erdos.ros.ros_service_op import ROSServiceOp
from erdos.ros.ros_subscriber_op import ROSSubscriberOp
from erdos.ros.ros_publisher_op import ROSPublisherOp
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency

from std_msgs.msg import String


FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_string('test_case', 'publisher',
                    'Test case: publisher | subscriber | service')


class AddTwoIntsInputOp(Op):
    def __init__(self, name='service_input_op'):
        super(AddTwoIntsInputOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=AddTwoIntsRequest, name='input_stream')]

    @frequency(1)
    def publish_inputs(self):
        request = AddTwoIntsRequest()
        request.a = random.randint(1, 10)
        request.b = random.randint(1, 10)
        msg = Message(request, Timestamp(coordinates=[0]))
        self.get_output_stream('input_stream').send(msg)

    def execute(self):
        self.publish_inputs()


class DataGeneratorOp(Op):
    def __init__(self, name):
        super(DataGeneratorOp, self).__init__(name)
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='data_gen_stream')]

    @frequency(1)
    def generate_data(self):
        msg = Message(str(self._cnt), Timestamp(coordinates=[self._cnt]))
        self.get_output_stream('data_gen_stream').send(msg)
        self._cnt += 1

    def execute(self):
        self.generate_data()


class EchoOp(Op):
    def __init__(self, name):
        super(EchoOp, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(EchoOp.on_msg)
        return []

    def on_msg(self, msg):
        print("Received {}".format(msg))


def main(argv):

    # Set up graph
    graph = erdos.graph.get_current_graph()

    if FLAGS.test_case == 'service':
        from beginner_tutorials.srv import *
        from beginner_tutorials.srv import AddTwoIntsRequest, AddTwoInts, AddTwoIntsResponse
        # Add operators
        add_two_ints_input_op = graph.add(AddTwoIntsInputOp, name='add_op')
        ros_service_op = graph.add(
            ROSServiceOp,
            name='ros_service',
            init_args={
                'service_name': 'add_two_ints',
                'srv_type': AddTwoInts
            },
            setup_args={
                'srv_ret_type': AddTwoIntsResponse,
                'op_name': 'add_two_ints'
            })

        # Connect graph
        graph.connect([add_two_ints_input_op], [ros_service_op])
    elif FLAGS.test_case == 'publisher':
        data_generator_op = graph.add(DataGeneratorOp, name='data_gen')
        ros_publisher_op = graph.add(
            ROSPublisherOp,
            name='ros_publisher',
            init_args={
                'ros_topic': 'my_topic',
                'data_type': String
            })
        graph.connect([data_generator_op], [ros_publisher_op])
    elif FLAGS.test_case == 'subscriber':
        ros_subscriber_op = graph.add(
            ROSSubscriberOp,
            name='ros_subscriber',
            init_args={
                'ros_topics_type': [('/my_topic', String, 'erdos_stream')]
            },
            setup_args={
                'ros_topics_type': [('/my_topic', String, 'erdos_stream')]
            })
        echo_op = graph.add(EchoOp, name='echo_op')
        graph.connect([ros_subscriber_op], [echo_op])
    else:
        raise ValueError('Unexpected test_case {}'.format(FLAGS.test_case))

    # Execute graph
    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
