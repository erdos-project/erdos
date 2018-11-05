from cv_bridge import CvBridge
from std_msgs.msg import Bool
from random import choice

from erdos.op import Op
from erdos.data_stream import DataStream
from erdos.message import Message


class MockPredictGripOperator(Op):
    """
    Mocks the prediction of the grip by randomly choosing between a successful
    or an unsuccessful grip.
    """
    success_stream_name = "grip-succ"
    fail_stream_name = "grip-fail"

    def __init__(self, name):
        """
        Initializes the images retrieved from the gripper.
        """
        super(MockPredictGripOperator, self).__init__(name)
        self.bridge = CvBridge()
        self.img_a, self.img_a_ini = None, None
        self.img_b, self.img_b_ini = None, None

    @staticmethod
    def setup_streams(input_streams, gel_sight_a_stream_name,
                      gel_sight_b_stream_name, trigger_stream_name):
        """
        Registers a callback on the given gel sight streams and returns a
        stream on which the prediction is sent.
        """
        input_streams.filter_name(gel_sight_a_stream_name)\
            .add_callback(MockPredictGripOperator.save_image_gelsight_a)
        input_streams.filter_name(gel_sight_b_stream_name) \
            .add_callback(MockPredictGripOperator.save_image_gelsight_b)
        input_streams.filter_name(trigger_stream_name)\
            .add_callback(MockPredictGripOperator.predict)
        return [
            DataStream(
                data_type=Bool,
                name=MockPredictGripOperator.success_stream_name),
            DataStream(
                data_type=Bool, name=MockPredictGripOperator.fail_stream_name)
        ]

    def save_image_gelsight_a(self, msg):
        """
        Save images from the first gelsight.
        """
        self.img_a_ini = self.img_a
        self.img_a = msg.data

    def save_image_gelsight_b(self, msg):
        """
        Save images from the second gelsight.
        """
        self.img_b_ini = self.img_b
        self.img_b = msg.data

    def predict(self, msg):
        """
        Mock a prediction by randomly choosing between success or failure.
        """
        prediction = choice([True, True])
        output_msg = Message(True, msg.timestamp)
        if prediction:
            self.get_output_stream(
                MockPredictGripOperator.success_stream_name).send(output_msg)
        else:
            self.get_output_stream(
                MockPredictGripOperator.success_stream_name).send(output_msg)

    def execute(self):
        self.spin()
