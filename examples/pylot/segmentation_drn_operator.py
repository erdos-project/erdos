import cv2
from cv_bridge import CvBridge
import drn.segment
from drn.segment import DRNSeg
from torch.autograd import Variable
import time
import torch

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

from sensor_msgs.msg import Image


class SegmentationDRNOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None):
        super(SegmentationDRNOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._output_stream_name = output_stream_name
        arch = "drn_d_22"
        classes = 19
        pretrained = "dependencies/data/drn_d_22_cityscapes.pth"
        self._pallete = drn.segment.CITYSCAPE_PALETTE
        self._bridge = CvBridge()
        self._model = DRNSeg(
            arch, classes, pretrained_model=None, pretrained=False)
        self._model.load_state_dict(torch.load(pretrained))
        # TODO(ionel): Automatically detect if GPU is available.
        if self._flags.segmentation_gpu:
            self._model = torch.nn.DataParallel(self._model).cuda()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        # Register a callback on the camera input stream.
        input_streams.add_callback(
            SegmentationDRNOperator.on_msg_camera_stream)
        return [DataStream(data_type=Image,
                           name=output_stream_name,
                           labels={'segmented': 'true'})]

    def on_msg_camera_stream(self, msg):
        """Camera stream callback method.
        Invoked upon the receipt of a message on the camera stream.
        """
        self._logger.info('%s received frame %s', self.name, msg.timestamp)
        start_time = time.time()
        image = self._bridge.imgmsg_to_cv2(msg.data, 'bgr8')
        image = torch.from_numpy(image.transpose([2, 0,
                                                  1])).unsqueeze(0).float()
        image_var = Variable(image, requires_grad=False, volatile=True)

        final = self._model(image_var)[0]
        _, pred = torch.max(final, 1)

        pred = pred.cpu().data.numpy()[0]
        img = self._pallete[pred.squeeze()]

        if self._flags.visualize_segmentation_output:
            cv2.imshow(self.name, img)
            cv2.waitKey(1)

        runtime = time.time() - start_time
        self._logger.info('DRN segmentation {} runtime {}'.format(
            self.name, runtime))

        output_msg = Message(img, msg.timestamp)
        self.get_output_stream(self._output_stream_name).send(output_msg)

    def execute(self):
        """Operator execute entry method."""
        # Ensures that the operator runs continuously.
        self.spin()
