from __future__ import print_function

from collections import deque
import numpy as np
import os
import sys

from carla.image_converter import to_rgb_array

import erdos
from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency


class FusionOperator(Op):
    """Fusion Operator

    Args:
        rgbd_max_range (float): Maximum distance of the rgbd frame
        camera_fov (float): Angular field of view in radians of the RGBD and
            RGB cameras used to infer depth information and generate bounding
            boxes respectively. Note that camera position, orientation, and
            FOV must be identical for both.
    """

    def __init__(self, name, camera_fov=np.pi / 4, rgbd_max_range=1000):
        super(FusionOperator, self).__init__(name)
        self._segments = []
        self._objs = []
        self.rgbd_max_range = rgbd_max_range
        self.camera_fov = camera_fov
        self.distances = None  # Synchronized

        self.car_positions = deque()
        self.distances = deque()
        self.objects = deque()

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter_name("vehicle_pos").add_callback(
            FusionOperator.update_pos)
        input_streams.filter_name("obj_stream").add_callback(
            FusionOperator.update_objects)
        input_streams.filter_name("depth").add_callback(
            FusionOperator.update_distances)
        return [DataStream(name='fusion_vehicles')]

    def calc_object_positions(self, object_bounds, distances, car_position,
                              car_orientation):
        # print(object_bounds, car_position, car_orientation, sep="\n")
        object_positions = []
        for bounds in object_bounds:
            x, y, w, h = bounds
            i_min, i_max, j_min, j_max = np.round(
                [x - w / 2, x + w / 2, y - w / 2, y + w / 2]).astype(np.int32)

            bounding_box_center = np.average([[i_min, i_max], [j_min, j_max]],
                                             axis=1)

            distance = np.median(distances[i_min:i_max, j_min:j_max])
            vertical_angle, horizontal_angle = (self.camera_fov * (
                bounding_box_center - distances.shape) / distances.shape)

            horizontal_diagonal = distance * np.cos(vertical_angle)

            forward_distance = horizontal_diagonal * np.cos(horizontal_angle)
            right_distance = horizontal_diagonal * np.sin(horizontal_angle)

            # TODO: check that this is right
            position_x = car_position[0] + forward_distance * np.cos(
                car_orientation) - right_distance * np.sin(car_orientation)
            position_y = car_position[1] + forward_distance * np.sin(
                car_orientation) - right_distance * np.cos(car_orientation)

            object_positions.append([position_x, position_y])

        return object_positions

    def discard_old_data(self):
        """Discards stored data that are too old to be used for fusion"""
        oldest_timestamp = min([
            self.car_positions[-1][0], self.distances[-1][0],
            self.objects[-1][0]
        ])
        for queue in [self.car_positions, self.distances, self.objects]:
            while queue[0][0] < oldest_timestamp:
                queue.popleft()

    @frequency(1)
    def fuse(self):
        if min(map(len,
                   [self.car_positions, self.distances, self.objects])) == 0:
            return
        self.discard_old_data()
        object_positions = self.calc_object_positions(
            self.objects[0][1], self.distances[0][1],
            self.car_positions[0][1][0],
            np.arccos(self.car_positions[0][1][1][0]))
        output_msg = Message(object_positions, self.objects[0][0])
        self.get_output_stream("fusion_vehicles").send(output_msg)

    def update_pos(self, msg):
        self.car_positions.append((msg.timestamp, msg.data))

    def update_objects(self, msg):
        # Filter objects
        vehicle_bounds = []
        for label, score, bounds in msg.data:
            if label in {"truck", "car"}:
                vehicle_bounds.append(bounds)
        self.objects.append((msg.timestamp, vehicle_bounds))

    def update_distances(self, msg):
        rgbd_frame = to_rgb_array(msg.data)
        normalized_distances = np.dot(
            rgbd_frame,
            [1.0, 256.0, 256.0 * 256.0]) / (256.0 * 256.0 * 256.0 - 1.0)
        distances = self.rgbd_max_range * normalized_distances
        self.distances.append((msg.timestamp, distances))

    def execute(self):
        self.fuse()
        self.spin()
