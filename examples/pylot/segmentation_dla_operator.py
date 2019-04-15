import cv2
from cv_bridge import CvBridge
import dla.DLASeg
import numpy as np
import PIL.Image as PILImage
import PIL.ImageDraw as ImageDraw
from sensor_msgs.msg import Image
import time
import torch

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from segmentation_utils import transfrom_to_cityscapes


class SegmentationDLAOperator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(SegmentationDLAOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._output_stream_name = output_stream_name
        self._bridge = CvBridge()
        # TODO(ionel): Figure out how to set GPU memory fraction.
        self._network = dla.DLASeg.DLASeg()
        self._network.load_state_dict(torch.load('dependencies/dla/DLASeg.pth'))
        if self._flags.segmentation_gpu:
            self._network = self._network.cuda()
        self._last_seq_num = -1

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        # Register a callback on the camera input stream.
        input_streams.add_callback(SegmentationDLAOperator.on_msg_camera_stream)
        return [DataStream(data_type=Image,
                           name=output_stream_name,
                           labels={'segmented': 'true'})]

    def on_msg_camera_stream(self, msg):
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        self._logger.info('%s received frame %s', self.name, msg.timestamp)
        start_time = time.time()
        image = self._bridge.imgmsg_to_cv2(msg.data, 'bgr8')
        image = np.expand_dims(image.transpose([2, 0, 1]), axis=0)
        tensor = torch.tensor(image).float().cuda() / 255.0
        output = self._network(tensor)
        output = transfrom_to_cityscapes(
            torch.argmax(output, dim=1).cpu().numpy()[0])
        pil_img = PILImage.fromarray(np.uint8(output)).convert('RGB')
        img = np.array(pil_img)
        
        if self._flags.visualize_segmentation_output:
            draw = ImageDraw.Draw(pil_img)
            draw.text((5, 5),
                      "Timestamp: {}".format(msg.timestamp),
                      fill='black')

            cv2.imshow(self.name, np.array(pil_img))
            cv2.waitKey(1)

        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},"{}",{}'.format(
            time_epoch_ms(), self.name, msg.timestamp, runtime))

        output_msg = Message(img, msg.timestamp)
        self.get_output_stream(self._output_stream_name).send(output_msg)

    def execute(self):
        self.spin()
