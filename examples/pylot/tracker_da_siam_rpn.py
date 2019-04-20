from collections import deque
import cv2
from cv_bridge import CvBridge
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import threading
import time

import torch
from DaSiamRPN.code.net import SiamRPNvot
from DaSiamRPN.code.run_SiamRPN import SiamRPN_init, SiamRPN_track

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

from detection_utils import add_bounding_box


class TrackerDaSiamRPN(Op):
    def __init__(self,
                 name,
                 output_stream_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(TrackerDaSiamRPN, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._output_stream_name = output_stream_name
        self._bridge = CvBridge()
        self._tracker = None
        # True when the tracker is ready to update bboxes.
        self._ready_to_update = False
        self._to_process = deque()
        # Initialize the siam network.
        self._siam_net = SiamRPNvot()
        self._siam_net.load_state_dict(torch.load('dependencies/DaSiamRPN/code/SiamRPNVOT.model'))
        self._siam_net.eval().cuda()
        self._last_seq_num = -1
        self._lock = threading.Lock()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        def is_obstacles_stream(stream):
            return stream.labels.get('obstacles', '') == 'true'

        def is_camera_stream(stream):
            return stream.labels.get('camera', '') == 'true'

        input_streams.filter(is_obstacles_stream).add_callback(
            TrackerDaSiamRPN.on_objects_msg)
        input_streams.filter(is_camera_stream).add_callback(
            TrackerDaSiamRPN.on_frame_msg)
        return [DataStream(name=output_stream_name)]

    def on_frame_msg(self, msg):
        # Ensure that we're not missing messages.
        if self._last_seq_num + 1 != msg.timestamp.coordinates[1]:
            self._logger.error('Expected msg with seq num {} but received {}'.format(
                (self._last_seq_num + 1), msg.timestamp.coordinates[1]))
            if self._flags.fail_on_message_loss:
                assert self._last_seq_num + 1 == msg.timestamp.coordinates[1]
        self._last_seq_num = msg.timestamp.coordinates[1]

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

    def on_objects_msg(self, msg):
        self._lock.acquire()
        self._ready_to_update = False
        self._logger.info("Received {} bboxes for {}".format(len(msg.data), msg.timestamp))
        # Remove frames that are older than the detector update.
        while len(self._to_process) > 0 and self._to_process[0][0] < msg.timestamp:
            self._logger.info("Removing stale {} {}".format(
                self._to_process[0][0], msg.timestamp))
            self._to_process.popleft()
        (detector_res, runtime) = msg.data
        corners = self.get_highest_confidence_pedestrian(detector_res)
        if corners is not None:
            if len(self._to_process) > 0:
                # Found the frame corresponding to the bounding boxes.
                (timestamp, frame) = self._to_process.popleft()
                assert timestamp == msg.timestamp
                # Re-start the tracker.
                width = corners[1] - corners[0]
                height = corners[3] - corners[2]
                target_pos = np.array([(corners[0] + corners[1]) / 2.0,
                                       (corners[2] + corners[3]) / 2.0])
                target_size = np.array([width, height])
                self._tracker = SiamRPN_init(frame, target_pos, target_size, self._siam_net)
                self._ready_to_update = True
                self._ready_to_update_timestamp = msg.timestamp
                self._logger.info(
                    'Restarting tracker with frame {}'.format(msg.timestamp))
                self._logger.info('Tracker has {} frames to catch-up'.format(len(self._to_process)))
                for (timestamp, frame) in self._to_process:
                    if self._ready_to_update:
                        self.__track_bboxes_on_frame(frame, timestamp, True)
            else:
                self._logger.info(
                    'Received bboxes update {}, but no frame to process'.format(
                        msg.timestamp))
        self._lock.release()

    def get_highest_confidence_pedestrian(self, detector_res):
        max_score = 0
        max_corners = None
        for (corners, score, label) in detector_res:
            if label == 'person' and score > max_score:
                max_corners = corners
                max_score = score
        return max_corners
                
    def execute(self):
        self.spin()

    def __track_bboxes_on_frame(self, frame, timestamp, catch_up):
        self._logger.info('Processing frame {}'.format(timestamp))
        self._tracker = SiamRPN_track(self._tracker, frame)
        if self._flags.visualize_tracker_output and not catch_up:
            img = Image.fromarray(np.uint8(frame)).convert('RGB')
            target_pos = self._tracker['target_pos']
            target_sz = self._tracker['target_sz']
            corners = (int(target_pos[0] - target_sz[0] / 2.0),
                       int(target_pos[0] + target_sz[0] / 2.0),
                       int(target_pos[1] - target_sz[1] / 2.0),
                       int(target_pos[1] + target_sz[1] / 2.0))
#            print(self._tracker)
            add_bounding_box(img, corners)
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
