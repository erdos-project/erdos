import json
import numpy as np
import PIL.Image as Image
from absl import app
from absl import flags
from collections import deque

import config
from control.ground_agent_operator import GroundAgentOperator
from perception.detection.utils import get_bounding_boxes_from_segmented, visualize_ground_bboxes
from perception.segmentation.utils import get_traffic_sign_pixels
from planning.waypointer_operator import WaypointerOperator
from simulation.carla_operator import CarlaOperator
from simulation.utils import get_2d_bbox_from_3d_box, get_camera_intrinsic_and_transform
import pylot_utils

import erdos.graph
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

FLAGS = flags.FLAGS

# Flags that control what data is recorded.
flags.DEFINE_string('data_path', 'data/',
                    'Path where to store Carla camera images')
flags.DEFINE_integer('log_every_nth_frame', 1,
                     'Control how often the script logs frames')


class CameraLoggerOp(Op):
    def __init__(self, name, flags, log_file_name=None, csv_file_name=None):
        super(CameraLoggerOp, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._last_bgr_timestamp = -1
        self._last_segmented_timestamp = -1

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(pylot_utils.is_camera_stream).add_callback(
            CameraLoggerOp.on_bgr_frame)
        input_streams.filter(
            pylot_utils.is_ground_segmented_camera_stream).add_callback(
                CameraLoggerOp.on_segmented_frame)
        return []

    def on_bgr_frame(self, msg):
        # Ensure we didn't skip a frame.
        if self._last_bgr_timestamp != -1:
            assert self._last_bgr_timestamp + 1 == msg.timestamp.coordinates[1]
        self._last_bgr_timestamp = msg.timestamp.coordinates[1]
        if self._last_bgr_timestamp % self._flags.log_every_nth_frame != 0:
            return
        # Write the image.
        assert msg.encoding == 'BGR', 'Expects BGR frames'
        rgb_array = pylot_utils.bgr_to_rgb(msg.frame)
        file_name = '{}carla-{}.png'.format(
            self._flags.data_path, self._last_bgr_timestamp)
        rgb_img = Image.fromarray(np.uint8(rgb_array))
        rgb_img.save(file_name)

    def on_segmented_frame(self, msg):
        # Ensure we didn't skip a frame.
        if self._last_segmented_timestamp != -1:
            assert self._last_segmented_timestamp + 1 == msg.timestamp.coordinates[1]
        self._last_segmented_timestamp = msg.timestamp.coordinates[1]
        if self._last_bgr_timestamp % self._flags.log_every_nth_frame != 0:
            return
        # Write the segmented image.
        img = Image.fromarray(np.uint8(msg.frame))
        file_name = '{}carla-segmented-{}.png'.format(
            self._flags.data_path, self._last_segmented_timestamp)
        img.save(file_name)


class GroundTruthObjectLoggerOp(Op):
    def __init__(self,
                 name,
                 rgb_camera_setup,
                 flags,
                 log_file_name=None,
                 csv_file_name=None):
        super(GroundTruthObjectLoggerOp, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._flags = flags
        # Queue of incoming data.
        self._bgr_imgs = deque()
        self._world_transforms = deque()
        self._depth_imgs = deque()
        self._pedestrians = deque()
        self._segmented_imgs = deque()
        self._vehicles = deque()
        (camera_name, pp, img_size, pos) = rgb_camera_setup
        (self._rgb_intrinsic, self._rgb_transform,
         self._rgb_img_size) = get_camera_intrinsic_and_transform(
             name=camera_name,
             postprocessing=pp,
             image_size=img_size,
             position=pos)
        self._last_notification = -1

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(pylot_utils.is_depth_camera_stream).add_callback(
            GroundTruthObjectLoggerOp.on_depth_camera_update)
        input_streams.filter(pylot_utils.is_camera_stream).add_callback(
            GroundTruthObjectLoggerOp.on_bgr_camera_update)
        input_streams.filter(
            pylot_utils.is_ground_segmented_camera_stream).add_callback(
                GroundTruthObjectLoggerOp.on_segmented_frame)
        input_streams.filter(
            pylot_utils.is_world_transform_stream).add_callback(
                GroundTruthObjectLoggerOp.on_world_transform_update)
        input_streams.filter(
            pylot_utils.is_ground_pedestrians_stream).add_callback(
                GroundTruthObjectLoggerOp.on_pedestrians_update)
        input_streams.filter(
            pylot_utils.is_ground_vehicles_stream).add_callback(
                GroundTruthObjectLoggerOp.on_vehicles_update)
        input_streams.add_completion_callback(
            GroundTruthObjectLoggerOp.on_notification)
        return []

    def on_notification(self, msg):
        # Check that we didn't skip any notification. We only skip
        # notifications if messages or watermarks are lost.
        if self._last_notification != -1:
            assert self._last_notification + 1 == msg.timestamp.coordinates[1]
        self._last_notification = msg.timestamp.coordinates[1]

        depth_msg = self._depth_imgs.popleft()
        bgr_msg = self._bgr_imgs.popleft()
        segmented_msg = self._segmented_imgs.popleft()
        world_trans_msg = self._world_transforms.popleft()
        pedestrians_msg = self._pedestrians.popleft()
        vehicles_msg = self._vehicles.popleft()
        self._logger.info('Timestamps {} {} {} {} {} {}'.format(
            depth_msg.timestamp, bgr_msg.timestamp, segmented_msg.timestamp,
            world_trans_msg.timestamp, pedestrians_msg.timestamp,
            vehicles_msg.timestamp))

        assert (depth_msg.timestamp == bgr_msg.timestamp ==
                segmented_msg.timestamp == world_trans_msg.timestamp ==
                pedestrians_msg.timestamp == vehicles_msg.timestamp)

        if self._last_notification % self._flags.log_every_nth_frame != 0:
            return

        depth_array = depth_msg.frame
        world_transform = world_trans_msg.data

        ped_bboxes = self.__get_pedestrians_bboxes(
            pedestrians_msg.pedestrians, world_transform, depth_array)

        vec_bboxes = self.__get_vehicles_bboxes(
            vehicles_msg.vehicles, world_transform, depth_array)

        traffic_sign_bboxes = self.__get_traffic_sign_bboxes(
            segmented_msg.frame)

        bboxes = ped_bboxes + vec_bboxes + traffic_sign_bboxes
        # Write the bounding boxes.
        file_name = '{}bboxes-{}.json'.format(
            self._flags.data_path, self._last_notification)
        with open(file_name, 'w') as outfile:
            json.dump(bboxes, outfile)

        if self._flags.visualize_ground_obstacles:
            ped_vis_bboxes = [(xmin, xmax, ymin, ymax)
                              for (_, ((xmin, ymin), (xmax, ymax))) in ped_bboxes]
            vec_vis_bboxes = [(xmin, xmax, ymin, ymax)
                              for (_, ((xmin, ymin), (xmax, ymax))) in vec_bboxes]
            traffic_sign_vis_bboxes = [(xmin, xmax, ymin, ymax)
                                        for (_, ((xmin, ymin), (xmax, ymax))) in traffic_sign_bboxes]
            visualize_ground_bboxes(self.name, bgr_msg.timestamp, bgr_msg.frame,
                                    ped_vis_bboxes, vec_vis_bboxes,
                                    traffic_sign_vis_bboxes)

    def on_world_transform_update(self, msg):
        self._world_transforms.append(msg)

    def on_pedestrians_update(self, msg):
        self._pedestrians.append(msg)

    def on_vehicles_update(self, msg):
        self._vehicles.append(msg)

    def on_depth_camera_update(self, msg):
        self._depth_imgs.append(msg)

    def on_bgr_camera_update(self, msg):
        self._bgr_imgs.append(msg)

    def on_segmented_frame(self, msg):
        self._segmented_imgs.append(msg)

    def execute(self):
        self.spin()

    def __get_pedestrians_bboxes(self, pedestrians, world_transform,
                                 depth_array):
        bboxes = []
        for pedestrian in pedestrians:
            bbox = get_2d_bbox_from_3d_box(
                depth_array, world_transform, pedestrian.transform,
                pedestrian.bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 1.5, 3.0)
            if bbox is not None:
                (xmin, xmax, ymin, ymax) = bbox
                bboxes.append(('pedestrian', ((xmin, ymin), (xmax, ymax))))
        return bboxes

    def __get_vehicles_bboxes(self, vehicles, world_transform, depth_array):
        vec_bboxes = []
        for vehicle in vehicles:
            bbox = get_2d_bbox_from_3d_box(
                depth_array, world_transform, vehicle.transform,
                vehicle.bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 3.0, 3.0)
            if bbox is not None:
                (xmin, xmax, ymin, ymax) = bbox
                vec_bboxes.append(('vehicle', ((xmin, ymin), (xmax, ymax))))
        return vec_bboxes

    def __get_traffic_sign_bboxes(self, segmented_frame):
        traffic_signs_frame = get_traffic_sign_pixels(segmented_frame)
        bboxes = get_bounding_boxes_from_segmented(traffic_signs_frame)
        traffic_sign_bboxes = [('traffic sign', bbox) for bbox in bboxes]
        return traffic_sign_bboxes


def add_carla_op(graph, camera_setups):
    carla_op = graph.add(
        CarlaOperator,
        name='carla',
        init_args={
            'flags': FLAGS,
            'camera_setups': camera_setups,
            'lidar_stream_names': [],
            'log_file_name': FLAGS.log_file_name,
            'csv_file_name': FLAGS.csv_log_file_name
        },
        setup_args={
            'camera_setups': camera_setups,
            'lidar_stream_names': []
        })
    return carla_op


def add_ground_agent_op(graph, carla_op):
    agent_op = graph.add(
        GroundAgentOperator,
        name='ground_agent',
        init_args={
            # TODO(ionel): Do not hardcode city name!
            'city_name': 'Town01',
            'flags': FLAGS,
            'log_file_name': FLAGS.log_file_name,
            'csv_file_name': FLAGS.csv_log_file_name
        })
    return agent_op


def add_waypointer_op(graph,
                      carla_op,
                      agent_op,
                      goal_location,
                      goal_orientation):
    waypointer_op = graph.add(
        WaypointerOperator,
        name='waypointer',
        init_args={
            'city_name': 'Town01',
            'goal_location': goal_location,
            'goal_orientation': goal_orientation,
            'flags': FLAGS,
            'log_file_name': FLAGS.log_file_name,
            'csv_file_name': FLAGS.csv_log_file_name
        })
    return waypointer_op


def main(argv):

    # Define graph
    graph = erdos.graph.get_current_graph()

    logging_ops = []
    # Add an operator that logs RGB frames and segmented frames.
    camera_logger_op = graph.add(
        CameraLoggerOp,
        name='camera_logger_op',
        init_args={'flags': FLAGS,
                   'log_file_name': FLAGS.log_file_name,
                   'csv_file_name': FLAGS.csv_log_file_name})
    logging_ops.append(camera_logger_op)

    rgb_camera_setup = ('front_rgb_camera',
                        'SceneFinal',
                        (FLAGS.carla_camera_image_width,
                         FLAGS.carla_camera_image_height),
                        (2.0, 0.0, 1.4))
    camera_setups = [rgb_camera_setup]

    # Depth camera is required to map from 3D object bounding boxes
    # to 2D.
    depth_camera_name = 'front_depth_camera'
    depth_camera_setup = (depth_camera_name, 'Depth',
                          (FLAGS.carla_camera_image_width,
                           FLAGS.carla_camera_image_height),
                          (2.0, 0.0, 1.4))
    camera_setups.append(depth_camera_setup)
    # Add operator that converts from 3D bounding boxes
    # to 2D bouding boxes.
    ground_object_logger_op = graph.add(
        GroundTruthObjectLoggerOp,
        name='ground_truth_obj_logger',
        init_args={'rgb_camera_setup': rgb_camera_setup,
                   'flags': FLAGS,
                   'log_file_name': FLAGS.log_file_name,
                   'csv_file_name': FLAGS.csv_log_file_name})
    logging_ops.append(ground_object_logger_op)

    # Record segmented frames as well. The CameraLoggerOp records them.
    segmentation_camera_setup = ('front_semantic_camera',
                                 'SemanticSegmentation',
                                 (FLAGS.carla_camera_image_width,
                                  FLAGS.carla_camera_image_height),
                                 (2.0, 0.0, 1.4))
    camera_setups.append(segmentation_camera_setup)

    # Add operator that interacts with the Carla simulator.
    carla_op = add_carla_op(graph, camera_setups)

    agent_op = add_ground_agent_op(graph, carla_op)

    # Add agent that uses ground data to drive around.
    goal_location = (234.269989014, 59.3300170898, 39.4306259155)
    goal_orientation = (1.0, 0.0, 0.22)
    waypointer_op = add_waypointer_op(graph,
                                      carla_op,
                                      agent_op,
                                      goal_location,
                                      goal_orientation)


    # Connect the operators.
    graph.connect([carla_op], [agent_op])
    graph.connect([agent_op], [carla_op])
    graph.connect([carla_op], logging_ops)
    graph.connect([carla_op], [waypointer_op])
    graph.connect([waypointer_op], [agent_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
