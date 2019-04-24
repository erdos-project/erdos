import cv2
import dla.DLASeg
import numpy as np
import time
import torch

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from segmentation_utils import transfrom_to_cityscapes
from pylot_utils import add_timestamp, create_segmented_camera_stream, is_camera_stream, rgb_to_bgr, bgra_to_bgr


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
        # TODO(ionel): Figure out how to set GPU memory fraction.
        self._network = dla.DLASeg.DLASeg()
        self._network.load_state_dict(torch.load('dependencies/dla/DLASeg.pth'))
        if self._flags.segmentation_gpu:
            self._network = self._network.cuda()
        self._last_seq_num = -1

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        # Register a callback on the camera input stream.
        input_streams.filter(is_camera_stream).add_callback(
            SegmentationDLAOperator.on_msg_camera_stream)
        return [create_segmented_camera_stream(output_stream_name)]

    def on_msg_camera_stream(self, msg):
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        self._logger.info('%s received frame %s', self.name, msg.timestamp)
        start_time = time.time()
        image = bgra_to_bgr(msg.data)
        image = np.expand_dims(image.transpose([2, 0, 1]), axis=0)
        tensor = torch.tensor(image).float().cuda() / 255.0
        output = self._network(tensor)
        # XXX(ionel): Check if the model outputs Carla Cityscapes values or
        # correct Cityscapes values.
        output = transfrom_to_cityscapes(
            torch.argmax(output, dim=1).cpu().numpy()[0])

        output = rgb_to_bgr(output)
        
        if self._flags.visualize_segmentation_output:
            add_timestamp(msg.timestamp, output)
            cv2.imshow(self.name, output)
            cv2.waitKey(1)

        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},"{}",{}'.format(
            time_epoch_ms(), self.name, msg.timestamp, runtime))

        output_msg = Message((output, runtime), msg.timestamp)
        self.get_output_stream(self._output_stream_name).send(output_msg)

    def execute(self):
        self.spin()
