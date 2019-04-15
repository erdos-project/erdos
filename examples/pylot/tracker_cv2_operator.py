from collections import deque
import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import time

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from utils import add_bounding_box

import threading

class TrackerCV2Operator(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(TrackerCV2Operator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._output_stream_name = output_stream_name
        self._bridge = CvBridge()
        self._tracker = cv2.MultiTracker_create()
        self._ready_to_update_timestamp = None
        self._to_process = deque()
        # True when the tracker is ready to update bboxes.
        self._ready_to_update = False
        self._last_seq_num = -1
        self._lock = threading.Lock()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        def is_obstacles_stream(stream):
            return stream.labels.get('obstacles', '') == 'true'

        def is_camera_stream(stream):
            return stream.labels.get('camera', '') == 'true'

        input_streams.filter(is_obstacles_stream).add_callback(
            TrackerCV2Operator.on_objects_msg)
        input_streams.filter(is_camera_stream).add_callback(
            TrackerCV2Operator.on_frame_msg)
        return [DataStream(name=output_stream_name)]

    def on_frame_msg(self, msg):
        # Ensure that we're not missing messages.
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

        self._logger.info("Received frame for {}".format(msg.timestamp))
        self._lock.acquire()
        start_time = time.time()
        frame = self._bridge.imgmsg_to_cv2(msg.data, 'rgb8')
        # Store frames so that they can be re-processed once we receive the
        # next update from the detector.
        self._to_process.append((msg.timestamp, frame))
        # Track if we have a tracker ready to accept new frames.
        if self._ready_to_update:
            self.__track_bboxes_on_frame(frame, msg.timestamp, False)
        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},"{}",{}'.format(
            time_epoch_ms(), self.name, msg.timestamp, runtime))
        self._lock.release()
        self._logger.info("on_frame_msg completed {}".format(msg.timestamp))

    def on_objects_msg(self, msg):
        self._lock.acquire()
        self._ready_to_update = False
        self._logger.info("Received {} bboxes for {}".format(len(msg.data), msg.timestamp))
        # Remove frames that are older than the detector update.
        while len(self._to_process) > 0 and self._to_process[0][0] < msg.timestamp:
            self._logger.info("Removing stale {} {}".format(
                self._to_process[0][0], msg.timestamp))
            self._to_process.popleft()
        if len(msg.data) > 0:
            if len(self._to_process) > 0:
                # Found the frame corresponding to the bounding boxes.
                (timestamp, frame) = self._to_process.popleft()
                assert timestamp == msg.timestamp
                # Re-start the tracker.
                self._tracker = cv2.MultiTracker_create()
                self._ready_to_update = True
                self._ready_to_update_timestamp = msg.timestamp
                self._logger.info(
                    'Restarting tracker with frame {}'.format(msg.timestamp))
                # Add a tracker for each bbox.
                for (corners, score, obstacle_class) in msg.data:
                    (xmin, xmax, ymin, ymax) = corners
                    bbox = (xmin, ymin, xmax - xmin, ymax - ymin)
                    self._tracker.add(cv2.TrackerKCF_create(), frame, bbox)
                    #self._tracker.add(cv2.TrackerMOSSE_create(), frame, bbox)
                # Catch-up.
                self._logger.info('Tracker has {} frames to catch-up'.format(len(self._to_process)))
                for (timestamp, frame) in self._to_process:
                    if self._ready_to_update:
                        self.__track_bboxes_on_frame(frame, timestamp, True)
            else:
                self._logger.info(
                    'Received bboxes update {}, but no frame to process'.format(
                        msg.timestamp))
        self._lock.release()
        self._logger.info("on_objects_msg completed {}".format(msg.timestamp))

    def execute(self):
        self.spin()

    def __track_bboxes_on_frame(self, frame, timestamp, catch_up):
        self._logger.info('Processing frame {}'.format(timestamp))
        ok, bboxes = self._tracker.update(frame)
        self._logger.info('Completed processing {}'.format(timestamp))
        if not ok:
            self._logger.error(
                'Tracker failed at timestamp {} last ready_to_update at {}'.format(
                    timestamp, self._ready_to_update_timestamp))
            # The tracker must be reinitialized.
            self._ready_to_update = False
        else:
            corners = []
            for (xmin, ymin, w, h) in bboxes:
                corners.append((xmin, xmin + w, ymin, ymin + h))

            if self._flags.visualize_tracker_output and not catch_up:
                img = Image.fromarray(np.uint8(frame)).convert('RGB')
                for c in corners:
                    add_bounding_box(img, c)
                draw = ImageDraw.Draw(img)
                draw.text((5, 5),
                          "Timestamp: {}".format(timestamp),
                          fill='black')
                open_cv_image = np.array(img)
                open_cv_image = open_cv_image[:, :, ::-1].copy()
                # XXX(ionel): For some reason cv2 hangs if we quickly send frames
                # one after another. This happens in the catch-up phase. I deactivated
                # visualizer during the catch-up phase.
                cv2.imshow(self.name, open_cv_image)
                cv2.waitKey(1)
            # TODO(ionel): We end up duplicating outputs. We output
            # when we first receive a frame, and again when the tracker
            # catches up after it receives an update from the detector.
            output_msg = Message(corners, timestamp)
            self.get_output_stream(
                self._output_stream_name).send(output_msg)
