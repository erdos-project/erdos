from absl import app
from absl import flags

import erdos.graph
from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.utils import frequency
from erdos.timestamp import Timestamp

try:
    from std_msgs.msg import String
except ModuleNotFoundError:
    # ROS not installed
    String = str

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')


class PublisherOperator(Op):
    def __init__(self, name, text):
        super(PublisherOperator, self).__init__(name)
        self.text = text
        self._cnt = 0

    @staticmethod
    def setup_streams(input_streams):
        return [DataStream(data_type=String, name='publisher')]

    @frequency(10)
    def publish_message(self):
        output_msg = Message("{}{}".format(self.text, self._cnt),
                             Timestamp(coordinates=[0]))
        self.get_output_stream("publisher").send(output_msg)
        self._cnt += 1

    def execute(self):
        self.publish_message()
        self.spin()


class SubscriberWithPrintOperator(Op):
    def __init__(self, name):
        super(SubscriberWithPrintOperator, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(SubscriberWithPrintOperator.receive_message)
        return []

    def receive_message(self, msg):
        print("Received message: {}".format(msg.data))

    def execute(self):
        self.spin()


class SubscriberWithoutPrintOperator(Op):
    def __init__(self, name):
        super(SubscriberWithoutPrintOperator, self).__init__(name)

    @staticmethod
    def setup_streams(input_streams):
        input_streams.add_callback(
            SubscriberWithoutPrintOperator.receive_message)
        return []

    def receive_message(self, msg):
        pass

    def execute(self):
        self.spin()


def main(argv):
    graph = erdos.graph.get_current_graph()
    publish_op_one = graph.add(
        PublisherOperator,
        name='publish_op_one',
        init_args={'text': 'publish-one'})
    publish_op_two = graph.add(
        PublisherOperator,
        name='publish_op_two',
        init_args={'text': 'publish-two'})
    subscriber_with_print_op = graph.add(
        SubscriberWithPrintOperator, name='subscriber_with_print')
    subscriber_without_print_op = graph.add(
        SubscriberWithoutPrintOperator, name='subscriber_without_print')
    graph.connect([publish_op_one], [subscriber_with_print_op])
    graph.connect([publish_op_two], [subscriber_without_print_op])

    graph.execute(FLAGS.framework)


if __name__ == "__main__":
    app.run(main)
