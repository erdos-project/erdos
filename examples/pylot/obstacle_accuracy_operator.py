from itertools import combinations
from cv_bridge import CvBridge
import cv2
import math
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import PIL.ImageFont as ImageFont

from carla.image_converter import depth_to_array
from carla.sensor import Camera

from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging

import messages
from utils import add_bounding_box, get_3d_world_position, map_ground_3D_transform_to_2D, map_ground_bounding_box_to_2D, point_cloud_from_rgbd


class ObstacleAccuracyOperator(Op):

    def __init__(self,
                 name, rgb_camera_name, depth_camera_name,
                 flags,
                 log_file_name=None,
                 csv_file_name=None,
                 rgbd_max_range=1000):
        super(ObstacleAccuracyOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._rgbd_max_range = rgbd_max_range
        self._bridge = CvBridge()
        self._world_transform = []
        self._pedestrians = []
        self._vehicles = []
        self._traffic_lights = []
        self._traffic_signs = []
        self._depth_imgs = []
        self._rgb_imgs = []
        (self._rgb_intrinsic, self._rgb_transform, self._rgb_img_size) = self.__setup_camera_tranforms(
            name=rgb_camera_name, postprocessing='SceneFinal')
        (self._depth_intrinsic, self._depth_transform, self._depth_img_size) = self.__setup_camera_tranforms(
            name=depth_camera_name, postprocessing='SemanticSegmentation')
        self.pedestrian_to_history_map = {}

    def __setup_camera_tranforms(self,
                                 name,
                                 postprocessing,
                                 field_of_view=90.0,
                                 image_size=(800, 600),
                                 position=(0.3, 0, 1.3),
                                 rotation_pitch=0,
                                 rotation_roll=0,
                                 rotation_yaw=0):
        camera = Camera(
            name,
            PostProcessing=postprocessing,
            FOV=field_of_view,
            ImageSizeX=image_size[0],
            ImageSizeY=image_size[1],
            PositionX=position[0],
            PositionY=position[1],
            PositionZ=position[2],
            RotationPitch=rotation_pitch,
            RotationRoll=rotation_roll,
            RotationYaw=rotation_yaw)

        image_width = image_size[0]
        image_height = image_size[1]
        # (Intrinsic) K Matrix
        intrinsic_mat = np.identity(3)
        intrinsic_mat[0][2] = image_width / 2
        intrinsic_mat[1][2] = image_height / 2
        intrinsic_mat[0][0] = intrinsic_mat[1][1] = image_width / (2.0 * math.tan(90.0 * math.pi / 360.0))
        return (intrinsic_mat, camera.get_unreal_transform(), (image_width, image_height))

    @staticmethod
    def setup_streams(input_streams, rgb_camera_name, depth_camera_name):
        def is_obstacles_stream(stream):
            return stream.labels.get('obstacles', '') == 'true'

        # XXX(ionel): This methos selects cameras from Carla
        # def is_rgb_camera_stream(stream):
        #     return (stream.labels.get('sensor_type', '') == 'camera' and
        #             stream.labels.get('camera_type', '') == 'SceneFinal')

        def is_ros_transformed_camera_stream(stream):
            return (stream.labels.get('camera', '') == 'true' and
                    stream.labels.get('ros', '') == 'true')

        input_streams.filter_name(depth_camera_name).add_callback(
            ObstacleAccuracyOperator.on_depth_camera_update)
        input_streams.filter(is_ros_transformed_camera_stream).add_callback(
            ObstacleAccuracyOperator.on_rgb_camera_update)
        input_streams.filter_name('world_transform').add_callback(
            ObstacleAccuracyOperator.on_world_transform_update)
        input_streams.filter_name('pedestrians').add_callback(
            ObstacleAccuracyOperator.on_pedestrians_update)
        input_streams.filter_name('vehicles').add_callback(
            ObstacleAccuracyOperator.on_vehicles_update)
        input_streams.filter_name('traffic_lights').add_callback(
            ObstacleAccuracyOperator.on_traffic_lights_update)
        input_streams.filter_name('traffic_signs').add_callback(
            ObstacleAccuracyOperator.on_traffic_signs_update)
        input_streams.filter(is_obstacles_stream).add_callback(
            ObstacleAccuracyOperator.on_obstacles)
        return []

    def on_world_transform_update(self, msg):
        self._world_transform.append(msg)

    def on_pedestrians_update(self, msg):
        self._pedestrians.append(msg)

    def on_vehicles_update(self, msg):
        self._vehicles.append(msg)

    def on_traffic_lights_update(self, msg):
        self._traffic_lights.append(msg)

    def on_traffic_signs_update(self, msg):
        self._traffic_signs.append(msg)

    def on_depth_camera_update(self, msg):
        self._depth_imgs.append(msg)

    def on_rgb_camera_update(self, msg):
        self._rgb_imgs.append(msg)

    def on_obstacles(self, msg):
        if (len(self._world_transform) == 0 or
            len(self._pedestrians) == 0 or
            len(self._vehicles) == 0 or
            len(self._traffic_lights) == 0 or
            len(self._traffic_signs) == 0 or
            len(self._depth_imgs) == 0 or
            len(self._rgb_imgs) == 0):
            return

        self._logger.info("Timestamps {} {} {} {} {} {} {}".format(
            self._world_transform[0].timestamp,
            self._pedestrians[0].timestamp,
            self._vehicles[0].timestamp,
            self._traffic_lights[0].timestamp,
            self._traffic_signs[0].timestamp,
            self._depth_imgs[0].timestamp,
            self._rgb_imgs[0].timestamp))

        timestamp = self._pedestrians[0].timestamp
        world_transform = self._world_transform[0].data
        self._world_transform = self._world_transform[1:]
        pedestrians = self._pedestrians[0].data
        self._pedestrians = self._pedestrians[1:]
        vehicles = self._vehicles[0].data
        self._vehicles = self._vehicles[1:]
        traffic_lights = self._traffic_lights[0].data
        self._traffic_lights = self._traffic_lights[1:]
        traffic_signs = self._traffic_signs[0].data
        self._traffic_signs = self._traffic_signs[1:]
        # NOTE: depth_to_array flips the image.
        depth_img = self._depth_imgs[0].data
        depth_array = depth_to_array(depth_img)
        self._depth_imgs = self._depth_imgs[1:]
        image_np = self._bridge.imgmsg_to_cv2(self._rgb_imgs[0].data, 'rgb8')
        rgb_img = Image.fromarray(np.uint8(image_np)).convert('RGB')
        self._rgb_imgs = self._rgb_imgs[1:]

        pedestrian_corners = []
        pedestrian_index = 1
        for (pd_transform, bounding_box, fwd_speed) in pedestrians:
            corners = map_ground_bounding_box_to_2D(rgb_img,
                                                   depth_array,
                                                   world_transform,
                                                   pd_transform,
                                                   bounding_box,
                                                   self._rgb_transform,
                                                   self._rgb_intrinsic,
                                                   self._rgb_img_size)

            if len(corners) == 8:
                self._logger.info(
                    "\n\nPrinting for pedestrian {} at timestamp {}".format(
                        pedestrian_index, timestamp))
                self._logger.info(
                    "The ground truth bounding box is \n{}".format(
                        bounding_box))

                # We get points in two planes. The second plane tracks the
                # objects better.
                corners_plane_2 = corners[4:]
                self._logger.info(
                    "The four corners in the second plane are: {}".format(
                        corners_plane_2))

                # Figure out the opposite ends of the rectangle.
                # Our 2D mapping doesn't return perfect rectangular coordinates
                # and also doesn't return them in clockwise order.
                max_distance = 0
                opp_ends_plane_2 = None
                for (a, b) in combinations(corners_plane_2, r=2):
                    if abs(a[0] - b[0]) <= 0.8 or abs(a[1] - b[1]) <= 0.8:
                        # The points are too close, they may be lying on the
                        # same axis. Move forward.
                        pass
                    else:
                        # The points do not possibly lie on the same axis.
                        # Choose the two points which are the farthest.
                        distance = (b[0] - a[0])**2 + (b[1] - a[1])**2
                        if distance > max_distance:
                            max_distance = distance
                            if a[0] < b[0] and a[1] < b[1]:
                                opp_ends_plane_2 = (a, b)
                            else:
                                opp_ends_plane_2 = (b, a)

                # Draw the image and mark it with the timestamp.
                draw = ImageDraw.Draw(rgb_img)
                draw.text((5, 5),
                          "Timestamp: {}".format(timestamp),
                          fill='black')

                # We were able to find two points far enough to be considered
                # as possible bounding boxes.
                if opp_ends_plane_2:
                    a, b = opp_ends_plane_2
                    self._logger.info(
                        "The minimum and the maximum in the second plane is: ")
                    self._logger.info("Point 1: {}".format(a))
                    self._logger.info("Point 2: {}".format(b))

                    # Find the middle point of the rectangle.
                    middle_point = ((a[0] + b[0]) / 2, (a[1] + b[1]) / 2,
                                    b[2].flatten().item(0))
                    if self.__inside_image(middle_point[0], middle_point[1],
                                           self._rgb_img_size[0],
                                           self._rgb_img_size[1]):
                        # The middle point was inside the image, sample around
                        # the image to see if the object inside is visible
                        # in our view.
                        sampling_points = [
                            (middle_point[0] + 3, middle_point[1],
                             middle_point[2]),
                            (middle_point[0] + 1.5, middle_point[1] + 1.5,
                             middle_point[2]),
                            (middle_point[0] + 1.5, middle_point[1] - 1.5,
                             middle_point[2]),
                            (middle_point[0] - 3, middle_point[1],
                             middle_point[2]),
                            (middle_point[0] - 1.5, middle_point[1] + 1.5,
                             middle_point[2]),
                            (middle_point[0] - 1.5, middle_point[1] - 1.5,
                             middle_point[2]),
                            (middle_point[0], middle_point[1], middle_point[2])
                        ]

                        # Remove all the sampling points that fall outside
                        # the image.
                        inside_sampling = []
                        for x, y, z in sampling_points:
                            if self.__inside_image(x, y,
                                                   self._rgb_img_size[0],
                                                   self._rgb_img_size[1]):
                                inside_sampling.append((x, y, z))

                        # Find the number of points that have a depth within
                        # a threshold from what is visible from our
                        # perspective.
                        results = [
                            self.have_same_depth(x, y, z, depth_array, 8.0)
                            for x, y, z in inside_sampling
                        ]

                        # BUG (sukritk) :: In the case of collisions, Carla
                        # does not update the pedestrian bounding box and
                        # we incorrectly draw the bounding box even if the
                        # pedestrian is not in our field of view until the
                        # world removes the pedestrian.
                        if self.pedestrian_to_history_map.get(
                                pedestrian_index):
                            # If the pedestrian was inside the image in the
                            # last timestamp, check if 40% of the points we
                            # sampled are inside the image.
                            if len(inside_sampling
                                   ) >= 0.4 * len(sampling_points):
                                pedestrian_corners.append((a[0], a[1], b[0],
                                                           b[1]))
                                draw.rectangle((tuple(map(int, a[:2])),
                                                tuple(map(int, b[:2]))),
                                               width=4,
                                               outline='green')
                                draw.text((a[0] + 1, a[1] + 1),
                                          str(pedestrian_index))
                                self._logger.info(
                                    "[{}, {}] Pedestrian transform is: {}".
                                    format(pedestrian_index, timestamp,
                                           pd_transform))
                            else:
                                # Majority of the bounding box is out of the
                                # image, do not draw the pedestrian.
                                self.pedestrian_to_history_map[
                                    pedestrian_index] = False
                        else:
                            # The pedestrian was not inside the image in the
                            # last timestamp. If more than 40% of the points
                            # inside the image have the same depth as visible
                            # from our viewpoint, update the map.
                            self.pedestrian_to_history_map[pedestrian_index] = \
                                    results.count(True) >= 0.4 * len(results)
                else:
                    self._logger.info(
                        "Could not find far enough points in second plane.")
            else:
                self._logger.info(
                    "The pedestrian {} returned no coordinates.".format(
                        pedestrian_index))
            pedestrian_index += 1

        # Compute the mIOU for the person category.
        detection_bboxes = [
            bbox for bbox, _, category in msg.data if category == 'person'
        ]
        if len(detection_bboxes) > 0 and len(pedestrian_corners) > 0:
            person_miou = self.__compute_miou(detection_bboxes,
                                              pedestrian_corners)
            self._logger.info(
                "The mIOU for the person category was: {}".format(person_miou))

        for (vec_transform, bounding_box, fwd_speed) in vehicles:
            corners = map_ground_bounding_box_to_2D(
                rgb_img, depth_array, world_transform, vec_transform,
                bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size)

            if len(corners) == 8:
                # The corners are represented in the cubic form and we
                # seperate the 2 planes to get the corners of the front
                # and the back rectangle.
                corners_plane_1, corners_plane_2 = corners[:4], corners[4:]

                # Figure out the lower-left and top-right corners from the
                # front plane.
                min_corner = corners_plane_1[0]
                max_corner_plane_1 = corners_plane_1[0]
                for corner in corners_plane_1[1:]:
                    if corner[0] < min_corner[0] + 1 and corner[
                            1] < min_corner[1] + 1:
                        min_corner = corner
                    if corner[0] > max_corner_plane_1[0] - 1 and corner[
                            1] > max_corner_plane_1[1] - 1:
                        max_corner_plane_1 = corner

                # Figure out the top right from the back plane.
                max_corner = corners_plane_2[0]
                for corner in corners_plane_2[1:]:
                    if corner[0] > max_corner[0] - 1 and corner[
                            1] > max_corner[1] - 1:
                        max_corner = corner

                # Find the middle point in the front plane.
                corner1, corner2 = min_corner, max_corner_plane_1
                middle_point = ((corner1[0] + corner2[0]) / 2,
                                (corner1[1] + corner2[1]) / 2,
                                corner1[2].flatten().item(0))
                if self.__inside_image(
                        middle_point[0], middle_point[1],
                        self._rgb_img_size[0],
                        self._rgb_img_size[1]) and self.have_same_depth(
                            middle_point[0], middle_point[1], middle_point[2],
                            depth_array, 8.0):

                    # Draw from the lower-left of the first plane to the top
                    # right of the back plane.
                    draw = ImageDraw.Draw(rgb_img)
                    draw.rectangle((corner1[:2], max_corner[:2]),
                                   width=4,
                                   outline='blue')

        for (tl_transform, state) in traffic_lights:
            pos = map_ground_3D_transform_to_2D(rgb_img,
                                                world_transform,
                                                self._rgb_transform,
                                                self._rgb_intrinsic,
                                                self._rgb_img_size,
                                                tl_transform)
            if pos is not None:
                x = int(pos[0])
                y = int(pos[1])
                z = pos[2].flatten().item(0)
                if self.have_same_depth(x, y, z, depth_array, 1.0):
                    # TODO(ionel): Figure out bounding box size.
                    add_bounding_box(rgb_img, (x - 2, x + 2, y - 2, y + 2), color='yellow')

        for (ts_transform, speed_sign) in traffic_signs:
            pos = map_ground_3D_transform_to_2D(rgb_img,
                                                world_transform,
                                                self._rgb_transform,
                                                self._rgb_intrinsic,
                                                self._rgb_img_size,
                                                ts_transform)
            if pos is not None:
                x = int(pos[0])
                y = int(pos[1])
                z = pos[2].flatten().item(0)
                #if self.have_same_depth(x, y, z, depth_array, 1.0):
                    # TODO(ionel): Figure out bounding box size.  add_bounding_box(rgb_img, (x - 2, x + 2, y - 2, y + 2), color='yellow') 
                # (x3d, y3d, z3d) = get_3d_world_position(
                #     x, y, self._depth_img_size, depth_img, self._depth_transform, world_transform)

        if self._flags.visualize_ground_obstacles:
            # Visualize bounding boxes.
            open_cv_image = np.array(rgb_img)
            open_cv_image = open_cv_image[:, :, ::-1].copy()
            cv2.imshow(self.name, open_cv_image)
            cv2.waitKey(1)

    def have_same_depth(self, x, y, z, depth_array, threshold):
        x, y = int(x), int(y)
        return abs(depth_array[y][x] * 1000 - z) < threshold

    def execute(self):
        self.spin()

    def __compute_miou(self, bboxes1, bboxes2):
        bboxes1, bboxes2 = np.array(bboxes1), np.array(bboxes2)
        x11, x12, y11, y12 = np.split(bboxes1, 4, axis=1)
        x21, y21, x22, y22 = np.split(bboxes2, 4, axis=1)

        xI1 = np.maximum(x11, np.transpose(x21))
        xI2 = np.minimum(x12, np.transpose(x22))

        yI1 = np.maximum(y11, np.transpose(y21))
        yI2 = np.minimum(y12, np.transpose(y22))

        inter_area = np.maximum((xI2 - xI1), 0) * np.maximum((yI2 - yI1), 0)

        bboxes1_area = (x12 - x11) * (y12 - y11)
        bboxes2_area = (x22 - x21) * (y22 - y21)

        union = (bboxes1_area + np.transpose(bboxes2_area)) - inter_area

        return inter_area / (union+0.0001)

    def __inside_image(self, x, y, img_width, img_height):
        return x >= 0 and y >= 0 and x < img_width and y < img_height

    def __compute_area(self, bbox):
        return (bbox[2] - bbox[0] + 1) * (bbox[3] - bbox[1] + 1)

    def __compute_accuracy(self, bbox1, bbox2):
        x1 = max(bbox1[0], bbox2[0])
        y1 = max(bbox1[1], bbox2[1])
        x2 = min(bbox1[2], bbox2[2])
        y2 = min(bbox1[3], bbox2[3])
        intersection_area = max(0, x2 - x1 + 1) * max(0, y2 - y1 + 1)
        bbox1_area = self.__compute_area(bbox1)
        bbox2_area = self.__compute_area(bbox2)
        iou = intersection_area / float(bbox1_area + bbox2_area - intersection_area)
        return iou
