from collections import deque
import numpy as np
import time

from carla.image_converter import to_rgb_array

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.utils import frequency, setup_csv_logging, setup_logging, time_epoch_ms

from pylot_utils import create_fusion_stream, is_depth_camera_stream, is_ground_vehicle_pos_stream, is_obstacles_stream


class FusionOperator(Op):
    """Fusion Operator

    Args:
        rgbd_max_range (float): Maximum distance of the rgbd frame
        camera_fov (float): Angular field of view in radians of the RGBD and
            RGB cameras used to infer depth information and generate bounding
            boxes respectively. Note that camera position, orientation, and
            FOV must be identical for both.
    """

    def __init__(self,
                 name,
                 flags,
                 output_stream_name,
                 log_file_name=None,
                 csv_file_name=None,
                 camera_fov=np.pi / 4,
                 rgbd_max_range=1000):
        super(FusionOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._flags = flags
        self._output_stream_name = output_stream_name
        self._segments = []
        self._objs = []
        self._rgbd_max_range = rgbd_max_range
        # TODO(ionel): Check fov is same as the camere fov.
        self._camera_fov = camera_fov
        self._car_positions = deque()
        self._distances = deque()
        self._objects = deque()

    @staticmethod
    def setup_streams(input_streams, output_stream_name):
        input_streams.filter(is_ground_vehicle_pos_stream).add_callback(
            FusionOperator.update_pos)
        input_streams.filter(is_obstacles_stream).add_callback(
            FusionOperator.update_objects)
        input_streams.filter(is_depth_camera_stream).add_callback(
            FusionOperator.update_distances)
        return [create_fusion_stream(output_stream_name)]

    def __calc_object_positions(self,
                                object_bounds,
                                distances,
                                car_position,
                                car_orientation):
        object_positions = []
        for bounds in object_bounds:
            i_min, i_max, j_min, j_max = bounds

            bounding_box_center = np.average(
                [[i_min, i_max], [j_min, j_max]], axis=1)

            distance = np.median(distances[i_min:i_max, j_min:j_max])
            vertical_angle, horizontal_angle = (self._camera_fov * (
                bounding_box_center - distances.shape) / distances.shape)

            horizontal_diagonal = distance * np.cos(vertical_angle)

            forward_distance = horizontal_diagonal * np.cos(horizontal_angle)
            right_distance = horizontal_diagonal * np.sin(horizontal_angle)

            # TODO(peter): check that this is right
            position_x = car_position[0] + forward_distance * np.cos(
                car_orientation) - right_distance * np.sin(car_orientation)
            position_y = car_position[1] + forward_distance * np.sin(
                car_orientation) - right_distance * np.cos(car_orientation)

            object_positions.append([position_x, position_y])

        return object_positions

    def __discard_old_data(self):
        """Discards stored data that are too old to be used for fusion"""
        oldest_timestamp = min([
            self._car_positions[-1][0], self._distances[-1][0],
            self._objects[-1][0]
        ])
        for queue in [self._car_positions, self._distances, self._objects]:
            while queue[0][0] < oldest_timestamp:
                queue.popleft()

    @frequency(1)
    def fuse(self):
        # Return if we don't have car position, distances or objects.
        start_time = time.time()
        if min(map(len, [self._car_positions,
                         self._distances,
                         self._objects])) == 0:
            return
        self.__discard_old_data()
        object_positions = self.__calc_object_positions(
            self._objects[0][1],
            self._distances[0][1],
            self._car_positions[0][1][0],
            np.arccos(self._car_positions[0][1][1][0]))
        timestamp = self._objects[0][0]

        # Get runtime in ms.
        runtime = (time.time() - start_time) * 1000
        self._csv_logger.info('{},{},{}'.format(
            time_epoch_ms(), self.name, runtime))

        output_msg = Message(object_positions, timestamp)
        self.get_output_stream(self._output_stream_name).send(output_msg)

    def update_pos(self, msg):
        self._car_positions.append((msg.timestamp, msg.data))

    def update_objects(self, msg):
        # Filter objects
        self._logger.info("Received update objects")
        vehicle_bounds = []
        (detector_res, runtime) = msg.data
        for corners, score, label in detector_res:
            self._logger.info("%s received: %s %s %s ",
                              self.name, corners, score, label)
            # TODO(ionel): Deal with different types of labels.
            if label in {"truck", "car"}:
                vehicle_bounds.append(corners)
        self._objects.append((msg.timestamp, vehicle_bounds))

    def update_distances(self, msg):
        rgbd_frame = to_rgb_array(msg.data)
        normalized_distances = np.dot(
            rgbd_frame,
            [1.0, 256.0, 256.0 * 256.0]) / (256.0 * 256.0 * 256.0 - 1.0)
        # Compute distances in meters.
        distances = self._rgbd_max_range * normalized_distances
        self._distances.append((msg.timestamp, distances))

    def execute(self):
        self.fuse()
        self.spin()
