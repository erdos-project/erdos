from absl import flags
import numpy as np
import PIL.Image as Image

from carla.image_converter import labels_to_cityscapes_palette

from erdos.op import Op
from erdos.utils import setup_logging

FLAGS = flags.FLAGS


class SegmentationEvalOperator(Op):

    def __init__(self, name):
        super(SegmentationEvalOperator, self).__init__(name)
        self._logger = setup_logging(self.name, FLAGS.log_file_name)
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
            self.compute_iou(ground_img, msg.data)
            index += 1
        if index > 0:
            self._ground_frames = self._ground_frames[index:]

    def compute_iou(self, ground_frame, predicted_frame):
        self._logger.info("Compute IOU {} {}".format(ground_frame.shape, predicted_frame.shape))

        # Cityscapes palette.
        classes = {
            0: [0, 0, 0],         # None
            1: [70, 70, 70],      # Buildings
            2: [190, 153, 153],   # Fences
            3: [72, 0, 90],       # Other
            4: [220, 20, 60],     # Pedestrians
            5: [153, 153, 153],   # Poles
            6: [157, 234, 50],    # RoadLines
            7: [128, 64, 128],    # Roads
            8: [244, 35, 232],    # Sidewalks
            9: [107, 142, 35],    # Vegetation
            10: [0, 0, 255],      # Vehicles
            11: [102, 102, 156],  # Walls
            12: [220, 220, 0]     # TrafficSigns
        }

        iou_scores = []
        for key, value in classes.items():
            target = np.zeros((ground_frame.shape[0], ground_frame.shape[1], 3))
            prediction = np.zeros((ground_frame.shape[0], ground_frame.shape[1], 3))
            target[np.where(ground_frame == value)] = 1
            prediction[np.where(predicted_frame == value)] = 1
            intersection = np.logical_and(target, prediction)
            union = np.logical_or(target, prediction)
            iou_scores.append(float(np.sum(intersection)) / float(np.sum(union)))
        self._logger.info("IoU score: {}".format(np.mean(iou_scores)))

    def execute(self):
        self.spin()
