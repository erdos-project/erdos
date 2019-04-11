from collections import deque
import numpy as np
import PIL.Image as Image

from carla.image_converter import labels_to_cityscapes_palette

from erdos.op import Op
from erdos.utils import setup_logging

from segmentation_utils import compute_semantic_iou


class SegmentationEvalGroundOperator(Op):

    def __init__(self, name, flags, log_file_name=None):
        super(SegmentationEvalGroundOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._time_delta = None
        self._ground_frames = deque()

    @staticmethod
    def setup_streams(input_streams, ground_stream_name):
        input_streams.filter_name(ground_stream_name).add_callback(
            SegmentationEvalGroundOperator.on_ground_segmented_frame)
        return []
        
    def on_ground_segmented_frame(self, msg):
        # We ignore the first several seconds of the simulation because the car
        # is not moving at beginning.
        if msg.timestamp.coordinates[0] > self._flags.eval_ground_truth_ignore_first:
            # Process the frame to cityscape pallete.
            ground_frame_array = labels_to_cityscapes_palette(msg.data)
            msg.data = np.array(
                Image.fromarray(np.uint8(ground_frame_array)).convert('RGB'))

            if len(self._ground_frames) > 0:
                if self._time_delta is None:
                    self._time_delta = (msg.timestamp.coordinates[0] -
                                        self._ground_frames[0].timestamp.coordinates[0])
                else:
                    # Check that no frames got dropped.
                    recv_time_delta = (msg.timestamp.coordinates[0] -
                                       self._ground_frames[-1].timestamp.coordinates[0])
                    assert self._time_delta == recv_time_delta

                # Pop the oldest frame if it's older than the max latency
                # we're interested in.
                if (msg.timestamp.coordinates[0] - self._ground_frames[0].timestamp.coordinates[0] >
                    self._flags.eval_ground_truth_max_latency):
                    self._ground_frames.popleft()

                for ground_frame in self._ground_frames:
                    (mean_iou, class_iou) = compute_semantic_iou(ground_frame.data, msg.data)
                    time_diff = msg.timestamp.coordinates[0] - ground_frame.timestamp.coordinates[0]
                    self._logger.info('Latency {}; Mean IoU {}'.format(time_diff, mean_iou))

            # Append the processed image to the buffer.
            self._ground_frames.append(msg)

    def execute(self):
        self.spin()
