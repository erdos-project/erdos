import os
import sys

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_logging

from sensor_msgs.msg import Image

import torch
from torch.autograd import Variable
import cv2
import drn.segment
from drn.segment import DRNSeg
from cv_bridge import CvBridge
from matplotlib import pyplot


class SegmentationOperator(Op):
    def __init__(self, name):
        super(SegmentationOperator, self).__init__(name)
        self._logger = setup_logging(self.name)
        self.arch = "drn_d_22"
        self.classes = 19
        self.pretrained = "dependencies/data/drn_d_22_cityscapes.pth"
        self.pallete = drn.segment.CITYSCAPE_PALETTE

    @staticmethod
    def setup_streams(input_streams):
        # Register a callback on the camera input stream.
        input_streams.add_callback(SegmentationOperator.on_msg_camera_stream)
        return [DataStream(data_type=Image, name='segment_stream')]

    def on_msg_camera_stream(self, msg):
        """Camera stream callback method.
        Invoked upon the receipt of a message on the camera stream.
        """
        self._logger.info('%s received frame %s', self.name, msg.timestamp)
        image = self.bridge.imgmsg_to_cv2(msg.data, "bgr8")
        image = torch.from_numpy(image.transpose([2, 0,
                                                  1])).unsqueeze(0).float()
        image_var = Variable(image, requires_grad=False, volatile=True)

        final = self.model(image_var)[0]
        _, pred = torch.max(final, 1)

        pred = pred.cpu().data.numpy()[0]
        img = self.pallete[pred.squeeze()]
        img = cv2.resize(img, (0, 0), fx=0.5, fy=0.5)

        # pyplot.imshow(img)
        # pyplot.show()
        output_msg = Message(img, msg.timestamp)


#        self.get_output_stream('segment_stream').send(output_msg)

    def execute(self):
        """Operator execute entry method."""
        # FIXME (yika): model should be initiated in __init__ because it is used in self._on_msg_camera_stream, which
        # might be called before execute
        self.bridge = CvBridge()
        self.model = DRNSeg(
            self.arch, self.classes, pretrained_model=None, pretrained=False)
        self.model.load_state_dict(torch.load(self.pretrained))
        # TODO(ionel): Uncomment when running on a GPU.
        # self.model = torch.nn.DataParallel(model).cuda()

        # Ensures that the operator runs continuously.
        self.spin()
