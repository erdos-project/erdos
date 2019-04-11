import numpy as np
import PIL.Image as Image

from carla.image_converter import labels_to_cityscapes_palette

from erdos.op import Op
from erdos.utils import setup_logging

from segmentation_utils import compute_semantic_iou


class SegmentationEvalOperator(Op):

    def __init__(self, name, log_file_name=None):
        super(SegmentationEvalOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._ground_frames = []
        self._segmented_frames = []

    @staticmethod
    def setup_streams(input_streams,
                      ground_stream_name,
                      segmented_stream_name):
        input_streams.filter_name(ground_stream_name).add_callback(
            SegmentationEvalOperator.on_ground_segmented_frame)
        input_streams.filter_name(segmented_stream_name).add_callback(
            SegmentationEvalOperator.on_segmented_frame)
        return []

    def on_ground_segmented_frame(self, msg):
        self._ground_frames.append(msg)

    def on_segmented_frame(self, msg):
        index = 0
        while (index < len(self._ground_frames) and
               self._ground_frames[index].timestamp < msg.timestamp):
            index += 1
        if index > 0:
            self._ground_frames = self._ground_frames[index:]
        index = 0
        while (index < len(self._ground_frames) and
               self._ground_frames[index].timestamp == msg.timestamp):
            ground_frame_array = labels_to_cityscapes_palette(
                self._ground_frames[index].data)
            ground_img = Image.fromarray(np.uint8(ground_frame_array)).convert('RGB')
            ground_img = np.array(ground_img)
            (mean_iou, class_iou) = compute_semantic_iou(ground_img, msg.data)
            self._logger.info("IoU class scores: {}".format(class_iou))
            self._logger.info("mean IoU score: {}".format(mean_iou))
            index += 1
        if index > 0:
            self._ground_frames = self._ground_frames[index:]

    def execute(self):
        self.spin()
