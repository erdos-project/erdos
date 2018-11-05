from sensor_msgs.msg import Image
from cv_bridge import CvBridge

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op


class GelSightOperator(Op):
    """
    Retrieves the message from the ROS topic and converts it to a CV2 image
    to be consumed by the prediction operator.
    """

    def __init__(self, name, output_name):
        """
        Initializes the output name to the given output name.
        """
        super(GelSightOperator, self).__init__(name)
        self.output_name = output_name

    @staticmethod
    def setup_streams(input_streams, input_name, output_name):
        """
        Filters the given stream from the input_streams and returns a single
        output stream which sends images from the gel sight camera.
        """
        input_streams.filter_name(input_name)\
            .add_callback(GelSightOperator.on_msg_image_stream)
        return [DataStream(data_type=Image, name=output_name)]

    def on_msg_image_stream(self, msg):
        """
        On receipt of a message, convert it to a CV2 image, and send it on the
        output stream.
        """
        img = self.bridge.imgmsg_to_cv2(msg.data, 'bgr8')
        img_msg = Message(img, msg.timestamp)
        self.get_output_stream(self.output_name).send(img_msg)

    def execute(self):
        self.bridge = CvBridge()
        self.spin()
