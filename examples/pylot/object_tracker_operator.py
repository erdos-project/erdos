from collections import deque
import cv2
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import threading
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from detection_utils import visualize_no_colors_bboxes
from utils import is_camera_stream, is_obstacles_stream


class ObjectTrackerOp(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 tracker_type,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(ObjectTrackerOp, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._output_stream_name = output_stream_name
        try:
            if tracker_type == 'cv2':
                from trackers.cv2_tracker import MultiObjectCV2Tracker
                self._tracker = MultiObjectCV2Tracker(self._flags)
            elif tracker_type == 'crv':
                from trackers.crv_tracker import MultiObjectCRVTracker
                self._tracker = MultiObjectCRVTracker(self._flags)
            elif tracker_type == 'da_siam_rpn':
                from trackers.da_siam_rpn_tracker import MultiObjectDaSiamRPNTracker
                self._tracker = MultiObjectDaSiamRPNTracker(self._flags)
            else:
                self._logger.fatal(
                    'Unexpected tracker type {}'.format(tracker_type))
        except ImportError:
            self._logger.fatal('Error importing {}'.format(tracker_type))
        # True when the tracker is ready to update bboxes.
        self._ready_to_update = False
        self._to_process = deque()
        self._last_seq_num = -1
        self._lock = threading.Lock()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams.filter(is_obstacles_stream).add_callback(
            ObjectTrackerOp.on_objects_msg)
        input_streams.filter(is_camera_stream).add_callback(
            ObjectTrackerOp.on_frame_msg)
        return [DataStream(name=output_stream_name)]

    def on_frame_msg(self, msg):
        # Ensure that we're not missing messages.
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error(
                'Expected msg with seq num {} but received {}'.format(
                    (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        self._lock.acquire()
        start_time = time.time()
        frame = msg.data
        # Store frames so that they can be re-processed once we receive the
        # next update from the detector.
        self._to_process.append((msg.timestamp, frame))
        # Track if we have a tracker ready to accept new frames.
        if self._ready_to_update:
            self.__track_bboxes_on_frame(frame, msg.timestamp, False)
        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},"{}",{}'.format(time_epoch_ms(),
                                                     self.name, msg.timestamp,
                                                     runtime))
        self._lock.release()

    def on_objects_msg(self, msg):
        self._lock.acquire()
        self._ready_to_update = False
        self._logger.info("Received {} bboxes for {}".format(
            len(msg.data), msg.timestamp))
        # Remove frames that are older than the detector update.
        while len(self._to_process
                  ) > 0 and self._to_process[0][0] < msg.timestamp:
            self._logger.info("Removing stale {} {}".format(
                self._to_process[0][0], msg.timestamp))
            self._to_process.popleft()
        (detector_res, runtime) = msg.data
        # bboxes = self.__get_highest_confidence_pedestrian(detector_res)
        # Track all pedestrians.
        bboxes = self.__get_pedestrians(detector_res)
        if len(bboxes) > 0:
            if len(self._to_process) > 0:
                # Found the frame corresponding to the bounding boxes.
                (timestamp, frame) = self._to_process.popleft()
                assert timestamp == msg.timestamp
                # Re-initialize trackers.
                self.__initialize_trackers(frame, bboxes, msg.timestamp)
                self._logger.info('Trackers have {} frames to catch-up'.format(
                    len(self._to_process)))
                for (timestamp, frame) in self._to_process:
                    if self._ready_to_update:
                        self.__track_bboxes_on_frame(frame, timestamp, True)
            else:
                self._logger.info(
                    'Received bboxes update {}, but no frame to process'.
                    format(msg.timestamp))
        self._lock.release()

    def execute(self):
        self.spin()

    def __get_highest_confidence_pedestrian(self, detector_res):
        max_score = 0
        max_corners = None
        for (corners, score, label) in detector_res:
            if label == 'person' and score > max_score:
                max_corners = corners
                max_score = score
        if max_corners:
            return [max_corners]
        else:
            return []

    def __get_pedestrians(self, detector_res):
        bboxes = []
        for (corners, score, label) in detector_res:
            if label == 'person':
                bboxes.append(corners)
        return bboxes

    def __initialize_trackers(self, frame, bboxes, timestamp):
        self._ready_to_update = True
        self._ready_to_update_timestamp = timestamp
        self._logger.info('Restarting trackers at frame {}'.format(timestamp))
        self._tracker.reinitialize(frame, bboxes)

    def __track_bboxes_on_frame(self, frame, timestamp, catch_up):
        self._logger.info('Processing frame {}'.format(timestamp))
        # Sequentually update state for each bounding box.
        ok, bboxes = self._tracker.track(frame)
        if not ok:
            self._logger.error(
                'Tracker failed at timestamp {} last ready_to_update at {}'.
                format(timestamp, self._ready_to_update_timestamp))
            # The tracker must be reinitialized.
            self._ready_to_update = False
        else:
            if self._flags.visualize_tracker_output and not catch_up:
                visualize_no_colors_bboxes(self.name, timestamp, frame, bboxes)
