import cv2
import json
import numpy as np
import PIL.Image as Image
import PIL.ImageDraw as ImageDraw
import PIL.ImageFont as ImageFont
from absl import app
from absl import flags
from cv_bridge import CvBridge
from collections import deque

from carla.image_converter import depth_to_array, labels_to_cityscapes_palette, to_rgb_array

import config
from carla_operator import CarlaOperator
from detection_utils import get_2d_bbox_from_3d_box, get_camera_intrinsic_and_transform
from ground_agent_operator import GroundAgentOperator
from video_operator import VideoOperator

import erdos.graph
from erdos.op import Op
from erdos.utils import setup_csv_logging, setup_logging, time_epoch_ms

FLAGS = flags.FLAGS

# Flags that control what data is recorded.
flags.DEFINE_string('data_path', 'data/',
                    'Path where to store Carla camera images')
flags.DEFINE_bool('record_segmentation', False,
                  'True to enable segmentation data recording')
flags.DEFINE_bool('record_bounding_boxes', False,
                  'True to enable object bounding boxes data recording')

def is_rgb_camera_stream(stream):
    return stream.labels.get('camera_type', '') == 'SceneFinal'


def is_segmented_camera_stream(stream):
    return stream.labels.get('camera_type', '') == 'SemanticSegmentation'


def is_depth_camera_stream(stream):
    return stream.labels.get('camera_type', '') == 'Depth'


class CameraLoggerOp(Op):
    def __init__(self, name, flags, log_file_name=None, csv_file_name=None):
        super(CameraLoggerOp, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self._last_rgb_timestamp = -1
        self._last_segmented_timestamp = -1

    @staticmethod
    def setup_streams(input_streams):
        input_streams.filter(is_rgb_camera_stream).add_callback(
            CameraLoggerOp.on_rgb_frame)
        input_streams.filter(is_segmented_camera_stream).add_callback(
            CameraLoggerOp.on_segmented_frame)
        return []

    def on_rgb_frame(self, msg):
        # Ensure we didn't skip a frame.
        if self._last_rgb_timestamp != -1:
            assert self._last_rgb_timestamp + 1 == msg.timestamp.coordinates[1]
        self._last_rgb_timestamp = msg.timestamp.coordinates[1]
        # Write the image.
        rgb_array = to_rgb_array(msg.data)
        file_name = '{}carla-{}.png'.format(
            self._flags.data_path, self._last_rgb_timestamp)
        rgb_img = Image.fromarray(np.uint8(rgb_array))
        rgb_img.save(file_name)
    
    def on_segmented_frame(self, msg):
        # Ensure we didn't skip a frame.
        if self._last_segmented_timestamp != -1:
            assert self._last_segmented_timestamp + 1 == msg.timestamp.coordinates[1]
        self._last_segmented_timestamp = msg.timestamp.coordinates[1]
        # Write the segmented image.
        frame_array = labels_to_cityscapes_palette(msg.data)
        img = Image.fromarray(np.uint8(frame_array)).convert('RGB')
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
        self._bridge = CvBridge()
        # Queue of incoming data.
        self._rgb_imgs = deque()
        self._world_transforms = deque()
        self._depth_imgs = deque()
        self._pedestrians = deque()
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
        input_streams.filter(is_depth_camera_stream).add_callback(
            GroundTruthObjectLoggerOp.on_depth_camera_update)
        input_streams.filter(is_rgb_camera_stream).add_callback(
            GroundTruthObjectLoggerOp.on_rgb_camera_update)
        input_streams.filter_name('world_transform').add_callback(
            GroundTruthObjectLoggerOp.on_world_transform_update)
        input_streams.filter_name('pedestrians').add_callback(
            GroundTruthObjectLoggerOp.on_pedestrians_update)
        input_streams.filter_name('vehicles').add_callback(
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
        rgb_msg = self._rgb_imgs.popleft()
        world_trans_msg = self._world_transforms.popleft()
        pedestrians_msg = self._pedestrians.popleft()
        vehicles_msg = self._vehicles.popleft()
        self._logger.info('Timestamps {} {} {} {} {}'.format(
            depth_msg.timestamp, rgb_msg.timestamp, world_trans_msg.timestamp,
            pedestrians_msg.timestamp, vehicles_msg.timestamp))

        assert (depth_msg.timestamp == rgb_msg.timestamp ==
                world_trans_msg.timestamp == pedestrians_msg.timestamp ==
                vehicles_msg.timestamp)
        
        rgb_array = to_rgb_array(rgb_msg.data)
        rgb_img = Image.fromarray(np.uint8(rgb_array))
        depth_array = depth_to_array(depth_msg.data)
        world_transform = world_trans_msg.data

        ped_bboxes = self.__get_pedestrians_bboxes(
            pedestrians_msg.data, rgb_img, world_transform, depth_array)

        vec_bboxes = self.__get_vehicles_bboxes(
            vehicles_msg.data, rgb_img, world_transform, depth_array)

        bboxes = ped_bboxes + vec_bboxes
        # Write the bounding boxes.
        file_name = '{}bboxes-{}.json'.format(
            self._flags.data_path, self._last_notification)
        with open(file_name, 'w') as outfile:
            json.dump(bboxes, outfile)

        if self._flags.visualize_ground_obstacles:
            # Draw the image and mark it with the timestamp.
            draw = ImageDraw.Draw(rgb_img)
            draw.text((5, 5),
                      "Timestamp: {}".format(msg.timestamp),
                      fill='black')
            for (_, corners) in ped_bboxes:
                draw.rectangle(corners, width=4, outline='green')
            for (_, corners) in vec_bboxes:
                draw.rectangle(corners, width=4, outline='blue')
            # Visualize bounding boxes.
            open_cv_image = np.array(rgb_img)
            open_cv_image = open_cv_image[:, :, ::-1]
            cv2.imshow(self.name, open_cv_image)
            cv2.waitKey(1)

    def on_world_transform_update(self, msg):
        self._world_transforms.append(msg)

    def on_pedestrians_update(self, msg):
        self._pedestrians.append(msg)

    def on_vehicles_update(self, msg):
        self._vehicles.append(msg)

    def on_depth_camera_update(self, msg):
        self._depth_imgs.append(msg)

    def on_rgb_camera_update(self, msg):
        self._rgb_imgs.append(msg)

    def execute(self):
        self.spin()

    def __get_pedestrians_bboxes(self, pedestrians, rgb_img, world_transform,
                                 depth_array):
        bboxes = []
        for (_, pd_transform, bounding_box, _) in pedestrians:
            bbox = get_2d_bbox_from_3d_box(
                rgb_img, depth_array, world_transform, pd_transform,
                bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 1.5, 3.0)
            if bbox is not None:
                (xmin, xmax, ymin, ymax) = bbox
                bboxes.append(('pedestrian', ((xmin, ymin), (xmax, ymax))))
        return bboxes

    def __get_vehicles_bboxes(self, vehicles, rgb_img, world_transform,
                              depth_array):
        vec_bboxes = []
        for (vec_transform, bounding_box, _) in vehicles:
            bbox = get_2d_bbox_from_3d_box(
                rgb_img, depth_array, world_transform, vec_transform,
                bounding_box, self._rgb_transform, self._rgb_intrinsic,
                self._rgb_img_size, 3.0, 3.0)
            if bbox is not None:
                (xmin, xmax, ymin, ymax) = bbox
                vec_bboxes.append(('vehicle', ((xmin, ymin), (xmax, ymax))))
        return vec_bboxes


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


def add_ground_agent_op(graph, carla_op, goal_location, goal_orientation):
    agent_op = graph.add(
        GroundAgentOperator,
        name='ground_agent',
        init_args={
            'city_name': 'Town01',
            'goal_location': goal_location,
            'goal_orientation': goal_orientation,
            'flags': FLAGS,
            'log_file_name': FLAGS.log_file_name,
            'csv_file_name': FLAGS.csv_log_file_name
        })
    return agent_op


def main(argv):

    # Define graph
    graph = erdos.graph.get_current_graph()

    logging_ops = []
    # Add an operator that logs RGB frames, and segmented frames if
    # --record_segmentation is enabled.
    camera_logger_op = graph.add(
        CameraLoggerOp,
        name='camera_logger_op',
        init_args={'flags': FLAGS,
                   'log_file_name': FLAGS.log_file_name,
                   'csv_file_name': FLAGS.csv_log_file_name})
    logging_ops.append(camera_logger_op)

    # We're always logging RGB frames.
    rgb_camera_setup = ('front_rgb_camera',
                        'SceneFinal',
                        (FLAGS.carla_camera_image_width,
                         FLAGS.carla_camera_image_height),
                        (2.0, 0.0, 1.4))
    camera_setups = [rgb_camera_setup]

    if FLAGS.record_bounding_boxes:
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

    if FLAGS.record_segmentation:
        # Record segmented frames as well. The CameraLoggerOp records them.
        segmentation_camera_setup = ('front_semantic_camera',
                                     'SemanticSegmentation',
                                     (FLAGS.carla_camera_image_width,
                                      FLAGS.carla_camera_image_height),
                                     (2.0, 0.0, 1.4))
        camera_setups.append(segmentation_camera_setup)


    # Add operator that interacts with the Carla simulator.
    carla_op = add_carla_op(graph, camera_setups)

    # Add agent that uses ground data to drive around.
    goal_location = (234.269989014, 59.3300170898, 39.4306259155)
    goal_orientation = (1.0, 0.0, 0.22)
    agent_op = add_ground_agent_op(graph,
                                   carla_op,
                                   goal_location,
                                   goal_orientation)

    # Connect the operators.
    graph.connect([carla_op], [agent_op])
    graph.connect([agent_op], [carla_op])
    graph.connect([carla_op], logging_ops)

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
