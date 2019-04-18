from cv_bridge import CvBridge
import cv2
import heapq
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import PIL.ImageFont as ImageFont

from carla.image_converter import depth_to_array

from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging

import messages
import detection_utils


class ObstacleAccuracyOperator(Op):

    def __init__(self,
                 name,
                 rgb_camera_setup,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(ObstacleAccuracyOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._bridge = CvBridge()
        self._world_transforms = []
        self._pedestrians = []
        self._vehicles = []
        self._traffic_lights = []
        self._traffic_signs = []
        self._depth_imgs = []
        self._rgb_imgs = []
        (camera_name, pp, img_size, pos) = rgb_camera_setup
        (self._rgb_intrinsic, self._rgb_transform, self._rgb_img_size) = detection_utils.get_camera_intrinsic_and_transform(
            name=camera_name, postprocessing=pp, image_size=img_size, position=pos)
        self._last_notification = -1
        # Buffer of detected obstacles.
        self._detected_obstacles = []
        # Buffer of ground truth bboxes.
        self._ground_obstacles = []
        # Heap storing pairs of (ground/output time, game time).
        self._detector_start_end_times = []
        self._sim_interval = None

    @staticmethod
    def setup_streams(input_streams, depth_camera_name):
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
        # Register a watermark callback.
        input_streams.add_completion_callback(
            ObstacleAccuracyOperator.on_notification)
        return []

    def on_notification(self, msg):
        # Check that we didn't skip any notification. We only skip
        # notifications if messages or watermarks are lost.
        if self._last_notification != -1:
            assert self._last_notification + 1 == msg.timestamp.coordinates[1]
        self._last_notification = msg.timestamp.coordinates[1]

        # Ignore the first two messages. We use them to get sim time
        # between frames.
        if self._last_notification < 2:
            if self._last_notification == 0:
                self._sim_interval = int(msg.timestamp.coordinates[0])
            elif self._last_notification == 1:
                # Set he real simulation interval.
                self._sim_interval = int(msg.timestamp.coordinates[0]) - self._sim_interval
            return

        game_time = msg.timestamp.coordinates[0]
        # Transform the 3D boxes at time watermark game time to 2D.
        (ped_bboxes, vec_bboxes) = self.__get_bboxes()
        # Add the pedestrians to the ground obstacles buffer.
        self._ground_obstacles.append((game_time, ped_bboxes))

        while len(self._detector_start_end_times) > 0:
            (end_time, start_time) = self._detector_start_end_times[0]
            # We can compute mAP if the endtime is not greater than the ground time.
            if end_time <= msg.timestamp.coordinates[0]:
                # This is the closest ground bounding box to the end time.
                heapq.heappop(self._detector_start_end_times)
                end_bboxes = self.__get_ground_obstacles_at(end_time)
                if self._flags.detection_eval_use_accuracy_model:
                    # Not using the detector's outputs => get ground bboxes.
                    start_bboxes = self.__get_ground_obstacles_at(start_time)
                    # TODO(ionel): Call function that computes mAP.
                    #compute_mAP(start_bboxes, end_bboxes)
                    self._logger.info('Start bboxes {}'.format(start_bboxes))
                    self._logger.info('End bboxes {}'.format(end_bboxes))
                else:
                    # Get detector output obstacles.
                    det_bboxes = self.__get_obstacles_at(start_time)
                    # TODO(ionel): Call function that computes mAP.
                    #compute_mAP(det_bboxes, end_bboxes)
                    self._logger.info('Detected bboxes {}'.format(det_bboxes))
                    self._logger.info('End bboxes {}'.format(end_bboxes))
                self._logger.info('Computing accuracy for {} {}'.format(
                    end_time, start_time))
            else:
                # The remaining entries require newer ground bboxes.
                break

        self.__garbage_collect_obstacles()

    def __get_ground_obstacles_at(self, timestamp):
        for (time, obstacles) in self._ground_obstacles:
            if time == timestamp:
                return obstacles
            elif time > timestamp:
                break
        self._logger.fatal(
            'Could not find ground obstacles for {}'.format(timestamp))

    def __get_obstacles_at(self, timestamp):
        for (time, obstacles) in self._detected_obstacles:
            if time == timestamp:
                return obstacles
            elif time > timestamp:
                break
        self._logger.fatal(
            'Could not find detected obstacles for {}'.format(timestamp))

    def __garbage_collect_obstacles(self):
        # Get the minimum watermark.
        watermark = None
        for (_, start_time) in self._detector_start_end_times:
            if watermark is None or start_time < watermark:
                watermark = start_time
        if watermark is None:
            return
        # Remove all detected obstacles that are below the watermark.
        index = 0
        while (index < len(self._detected_obstacles) and
               self._detected_obstacles[index][0] < watermark):
            index += 1
        if index > 0:
            self._detected_obstacles = self._detected_obstacles[index:]
        # Remove all the ground obstacles that are below the watermark.
        index = 0
        while (index < len(self._ground_obstacles) and
               self._ground_obstacles[index][0] < watermark):
            index += 1
        if index > 0:
            self._ground_obstacles = self._ground_obstacles[index:]

    def on_world_transform_update(self, msg):
        if msg.timestamp.coordinates[1] >= 2:
            self._world_transforms.append(msg)

    def on_pedestrians_update(self, msg):
        if msg.timestamp.coordinates[1] >= 2:
            self._pedestrians.append(msg)

    def on_vehicles_update(self, msg):
        if msg.timestamp.coordinates[1] >= 2:
            self._vehicles.append(msg)

    def on_traffic_lights_update(self, msg):
        if msg.timestamp.coordinates[1] >= 2:
            self._traffic_lights.append(msg)

    def on_traffic_signs_update(self, msg):
        if msg.timestamp.coordinates[1] >= 2:
            self._traffic_signs.append(msg)

    def on_depth_camera_update(self, msg):
        if msg.timestamp.coordinates[1] >= 2:
            self._depth_imgs.append(msg)

    def on_rgb_camera_update(self, msg):
        if msg.timestamp.coordinates[1] >= 2:
            self._rgb_imgs.append(msg)

    def on_obstacles(self, msg):
        # Ignore the first two messages. We use them to get sim time
        # between frames.
        if msg.timestamp.coordinates[1] < 2:
            return
        (bboxes, runtime) = msg.data
        game_time = msg.timestamp.coordinates[0]
        self._detected_obstacles.append((game_time, bboxes))
        # Two metrics: 1) mAP, and 2) timely-mAP
        if self._flags.eval_detection_metric == 'mAP':
            # We will compare the bboxes with the ground truth at the same
            # game time.
            heapq.heappush(self._detector_start_end_times, (game_time, game_time))
        elif self._flags.eval_detection_metric == 'timely-mAP':
            # Ground bboxes time should be as close as possible to the time of the
            # obstacles + detector runtime.
            ground_bboxes_time = game_time + runtime
            if self._flags.detection_eval_use_accuracy_model:
                # Include the decay of detection with time if we do not want to use
                # the accuracy of our models.
                ground_bboxes_time += self.__mAP_to_latency(1)
            ground_bboxes_time = self.__compute_closest_frame_time(ground_bboxes_time)
            # Round time to nearest frame.
            heapq.heappush(self._detector_start_end_times,
                           (ground_bboxes_time, game_time))
        else:
            self._logger.fatal('Unexpected detection metric {}'.format(
                self._flags.eval_detection_metric))

    def execute(self):
        self.spin()

    def __compute_closest_frame_time(self, time):
        base = int(time) / self._sim_interval * self._sim_interval
        if time - base < self._sim_interval / 2:
            return base
        else:
            return base + self._sim_interval

    def __get_bboxes(self):
        self._logger.info("Timestamps {} {} {} {} {} {} {}".format(
            self._world_transforms[0].timestamp,
            self._pedestrians[0].timestamp,
            self._vehicles[0].timestamp,
            self._traffic_lights[0].timestamp,
            self._traffic_signs[0].timestamp,
            self._depth_imgs[0].timestamp,
            self._rgb_imgs[0].timestamp))

        timestamp = self._pedestrians[0].timestamp
        world_transform = self._world_transforms[0].data
        self._world_transforms = self._world_transforms[1:]

        # Get the latest RGB and depth images.
        # NOTE: depth_to_array flips the image.
        depth_img = self._depth_imgs[0].data
        depth_array = depth_to_array(depth_img)
        self._depth_imgs = self._depth_imgs[1:]
        image_np = self._bridge.imgmsg_to_cv2(self._rgb_imgs[0].data, 'rgb8')
        rgb_img = Image.fromarray(np.uint8(image_np)).convert('RGB')
        self._rgb_imgs = self._rgb_imgs[1:]

        # Get bboxes for pedestrians.
        pedestrians = self._pedestrians[0].data
        self._pedestrians = self._pedestrians[1:]
        ped_bbox_id = self.__get_pedestrians_bboxes(
            pedestrians, rgb_img, world_transform, depth_array)

        # Get bboxes for vehicles.
        vehicles = self._vehicles[0].data
        self._vehicles = self._vehicles[1:]
        vec_bboxes = self.__get_vehicles_bboxes(
            vehicles, rgb_img, world_transform, depth_array)

        # Get bboxes for traffic lights.
        traffic_lights = self._traffic_lights[0].data
        self._traffic_lights = self._traffic_lights[1:]
        # self.__get_traffic_light_bboxes(traffic_lights, rgb_img,
        #                                 world_transform, depth_array)

        # Get bboxes for the traffic signs.
        traffic_signs = self._traffic_signs[0].data
        self._traffic_signs = self._traffic_signs[1:]
        # self.__get_traffic_sign_bboxes(traffic_signs, rgb_img,
        #                                world_transform, depth_array)

        if self._flags.visualize_ground_obstacles:
            # Draw the image and mark it with the timestamp.
            draw = ImageDraw.Draw(rgb_img)
            draw.text((5, 5),
                      "Timestamp: {}".format(timestamp),
                      fill='black')
            for (pedestrian_id, corners) in ped_bbox_id:
                (xmin, xmax, ymin, ymax) = corners
                draw.rectangle(((xmin, ymin), (xmax, ymax)),
                               width=4,
                               outline='green')
                draw.text((xmin + 1, ymin + 1), str(pedestrian_id))

            for (xmin, xmax, ymin, ymax) in vec_bboxes:
                draw.rectangle(((xmin, ymin), (xmax, ymax)),
                               width=4,
                               outline='blue')
            # Visualize bounding boxes.
            open_cv_image = np.array(rgb_img)
            open_cv_image = open_cv_image[:, :, ::-1].copy()
            cv2.imshow(self.name, open_cv_image)
            cv2.waitKey(1)

        ped_bboxes = [bbox for (_, bbox) in ped_bbox_id]
        return (ped_bboxes, vec_bboxes)

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

    def __get_traffic_light_bboxes(self, traffic_lights, rgb_img,
                                   world_transform, depth_array):
        for (tl_transform, state) in traffic_lights:
            pos = detection_utils.map_ground_3D_transform_to_2D(rgb_img,
                                                                world_transform,
                                                                self._rgb_transform,
                                                                self._rgb_intrinsic,
                                                                self._rgb_img_size,
                                                                tl_transform)
            if pos is not None:
                x = int(pos[0])
                y = int(pos[1])
                z = pos[2].flatten().item(0)
                if detection_utils.have_same_depth(x, y, z, depth_array, 1.0):
                    # TODO(ionel): Figure out bounding box size.
                    detection_utils.add_bounding_box(rgb_img,
                                                     (x - 2, x + 2, y - 2, y + 2),
                                                     color='yellow')

    def __get_traffic_sign_bboxes(self, traffic_signs, rgb_img,
                                  world_transform, depth_array):
        for (ts_transform, speed_sign) in traffic_signs:
            pos = detection_utils.map_ground_3D_transform_to_2D(rgb_img,
                                                                world_transform,
                                                                self._rgb_transform,
                                                                self._rgb_intrinsic,
                                                                self._rgb_img_size,
                                                                ts_transform)
            if pos is not None:
                x = int(pos[0])
                y = int(pos[1])
                z = pos[2].flatten().item(0)
                if detection_utils.have_same_depth(x, y, z, depth_array, 1.0):
                    # TODO(ionel): Figure out bounding box size.
                    detection_utils.add_bounding_box(rgb_img,
                                                     (x - 2, x + 2, y - 2, y + 2),
                                                     color='yellow')

    def __get_pedestrians_bboxes(self, pedestrians, rgb_img, world_transform,
                                 depth_array):
        ped_bbox_id = []
        for (pedestrian_index, pd_transform, bounding_box,
             fwd_speed) in pedestrians:
            bbox = detection_utils.get_2d_bbox_from_3d_box(
                rgb_img, depth_array, world_transform, pd_transform,
                bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 1.5, 3.0)
            if bbox is not None:
                ped_bbox_id.append((pedestrian_index, bbox))
        return ped_bbox_id

    def __get_vehicles_bboxes(self, vehicles, rgb_img, world_transform,
                              depth_array):
        vec_bboxes = []
        for (vec_transform, bounding_box, fwd_speed) in vehicles:
            bbox = detection_utils.get_2d_bbox_from_3d_box(
                rgb_img, depth_array, world_transform, vec_transform,
                bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 3.0, 3.0)
            if bbox is not None:
                vec_bboxes.append(bbox)
        return vec_bboxes

    def __mAP_to_latency(self, mAP):
        """ Function that gives a latency estimate of how much simulation
        time must pass for a perfect detector to decay to mAP.
        """
        # TODO(ionel): Implement!
        return 0
