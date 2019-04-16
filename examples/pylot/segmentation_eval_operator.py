import numpy as np
import PIL.Image as Image

from carla.image_converter import labels_to_cityscapes_palette

from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from segmentation_utils import compute_semantic_iou


class SegmentationEvalOperator(Op):

    def __init__(self, name, flags, log_file_name=None, csv_file_name=None):
        super(SegmentationEvalOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._last_seq_num_ground_segmented = -1
        self._last_seq_num_segmented = -1
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
        if self._last_seq_num_ground_segmented + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num_ground_segmented + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num_ground_segmented + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num_ground_segmented = msg.timestamp.coordinates[1]
        # Buffer the ground truth frames.
        self._ground_frames.append(msg)

    def on_segmented_frame(self, msg):
        if self._last_seq_num_segmented + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num_segmented + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num_segmented + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num_segmented = msg.timestamp.coordinates[1]

        # Two metrics: 1) mIoU, and 2) timely-mIoU
        if self._flags.eval_segmentation_metric == 'mIoU':
            # Trim all state that is older than timestamp.
            self.__trim_frame_state_up_to(msg.timestamp.coordinates[0])
            # Get the frame that has the same timestamp as the segmented frame.
            ground_frame = self.__find_exact_ground_frame(msg.timestamp)
            if ground_frame is not None:
                self.__compute_mean_iou(ground_frame, msg)
        elif self._flags.eval_segmentation_metric == 'timely-mIoU':
            (frame, runtime) = msg.data
            game_time = msg.timestamp.coordinates[0]
            # Ground frame time should be as close as possible to the time of the
            # segmented frame + segmentation runtime.
            ground_frame_time = game_time + runtime
            if self._flags.segmentation_eval_use_accuracy_model:
                # Include the decay of the segmentation model with time
                # ifa we do not want to use the accuracy of our models.
                ground_frame_time += self.__mean_iou_to_latency(1)

            # Trim state up to game time.
            self.__trim_frame_state_up_to(game_time)

            # Find the closest frame to ground_frame_time.
            ground_frame = self.__find_approx_ground_frame(ground_frame_time)
            if ground_frame is not None:
                self.__compute_mean_iou(ground_frame, msg)
        else:
            self._logger.fatal('Unexpected segmentation metric {}'.format(
                self._flags.eval_segmentation_metric))

    def execute(self):
        self.spin()

    def __trim_frame_state_up_to(self, timestamp):
        index = 0
        while (index < len(self._ground_frames) and
               self._ground_frames[index].timestamp.coordinates[0] < timestamp):
            index += 1
        # Remove the ground frames that are  older than the timestamp.
        if index > 0:
            self._ground_frames = self._ground_frames[index:]

    def __find_exact_ground_frame(self, timestamp):
        # If there are frames left then the first one should have a matching
        # timestamp.
        if len(self._ground_frames) > 0:
            assert self._ground_frames[0].timestamp == timestamp
            frame = self._ground_frames[0].data
            # Remove the frame.
            self._ground_frames = self._ground_frames[1:]
            return frame
        self._logger.error('Could not find ground frame for {}'.format(timestamp))

    def __find_approx_ground_frame(self, timestamp):
        min_index = 0
        min_drift = None
        index = 0
        for msg in self._ground_frames:
            if min_drift is None:
                min_drift = abs(timestamp - msg.timestamp.coordinates[0])
                min_index = index
            elif abs(timestamp - msg.timestamp.coordinates[0]) < min_drift:
                min_drift = abs(timestamp - msg.timestamp.coordinates[0])
                min_index = index
            index += 1
        if min_drift is not None:
            self._logger.info('Minimum ground frame drift {}'.format(min_drift))
            return self._ground_frames[min_index].data
        self._logger.error('Could not find any ground frame for {}'.format(timestamp))

    def __compute_mean_iou(self, ground_frame, msg):
        ground_frame_array = labels_to_cityscapes_palette(
            self._ground_frames[0].data)
        ground_img = Image.fromarray(np.uint8(ground_frame_array)).convert('RGB')
        ground_img = np.array(ground_img)
        (frame, runtime) = msg.data
        (mean_iou, class_iou) = compute_semantic_iou(ground_img, frame)
        self._logger.info('IoU class scores: {}'.format(class_iou))
        self._logger.info('mean IoU score: {}'.format(mean_iou))
        self._csv_logger.info('{},{},"{}",{}'.format(
            time_epoch_ms(), self.name, msg.timestamp, mean_iou))

    def __mean_iou_to_latency(self, mean_iou):
        """ Function that gives a latency estimate of how much
        simulation time must pass such that a 1.0 IoU decays to mean_iou.
        """
        # TODO(ionel): Implement!
        return 0
