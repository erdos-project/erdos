import os
import sys
import time
from absl import app
from absl import flags

from agent_operator import AgentOperator
from camera_replay_operator import CameraReplayOperator
from carla_operator import CarlaOperator
from carla_to_image_operator import CarlaToImageOperator
from control_operator import ControlOperator
from detection_operator import DetectionOperator
from fusion_operator import FusionOperator
from fusion_verification_operator import FusionVerificationOperator
from lidar_visualizer_op import LidarVisualizerOperator
from object_tracker_operator import ObjectTrackerOperator
from planner.planner_operator import PlannerOperator
from segmentation_operator import SegmentationOperator
from segmented_video_operator import SegmentedVideoOperator
from traffic_light_det_operator import TrafficLightDetOperator
from video_operator import VideoOperator

import erdos.graph
from erdos.operators import RecordOp
from erdos.operators import ReplayOp

FLAGS = flags.FLAGS
flags.DEFINE_string('framework', 'ros',
                    'Execution framework to use: ros | ray.')
flags.DEFINE_bool('replay', False,
                  ('True if run in replay mode, otherwise run '
                   'Carla in server mode using `./CarlaUE4.sh -carla-server`'))
flags.DEFINE_bool('segmentation', False,
                  'True to enable segmantation operator')
flags.DEFINE_bool('obj_detection', False,
                  'True to enable object detection operator')
flags.DEFINE_bool('obj_tracking', False,
                  'True to enable object tracking operator')
flags.DEFINE_bool('fusion', False, 'True to enable fusion operator')
flags.DEFINE_bool('traffic_light_det', False,
                  'True to enable traffic light detection operator')
flags.DEFINE_bool('carla_segmented', False,
                  'True to enable CARLA segmented video operator')
flags.DEFINE_bool('lidar', False,
                  'True to enable CARLA Lidar visualizer operator')
flags.DEFINE_bool('rgb_camera_video', True,
                  'True to enable RGB camera video operator')
flags.DEFINE_bool('depth_camera_video', True,
                  'True to enable depth camera video operator')
flags.DEFINE_bool('record_rgb_camera', False, 'True to record RGB camera')
flags.DEFINE_bool('record_depth_camera', False, 'True to record depth camera')
flags.DEFINE_bool('record_lidar', False, 'True to record lidar')
flags.DEFINE_bool(
    'record_ground_truth', False,
    'True to carla data (e.g., vehicle position, traffic lights)')
flags.DEFINE_integer('num_cameras', 5, 'Number of cameras.')
flags.DEFINE_string('carla_host', 'localhost', 'Carla host.')
flags.DEFINE_integer('carla_port', 2000, 'Carla port.')
flags.DEFINE_integer('carla_num_vehicles', 10, 'Carla num vehicles.')
flags.DEFINE_integer('carla_num_pedestrians', 20, 'Carla num pedestrians.')


def main(argv):

    # Define graph
    graph = erdos.graph.get_current_graph()

    if FLAGS.replay:
        # Create camera operators.
        camera_ops = []
        for i in range(0, FLAGS.num_cameras, 1):
            op_name = 'camera{}'.format(i)
            camera_op = graph.add(
                CameraReplayOperator,
                name=op_name,
                setup_args={'op_name': op_name})
            camera_ops.append(camera_op)

        # replay_rgb_op = ReplayOp('pylot_rgb_camera_data.erdos',
        #                          frequency=10,
        #                          name='replay_rgb_camera')
        # camera_streams = replay_rgb_op([])
    else:
        # Define variables
        goal_location = (234.269989014, 59.3300170898, 39.4306259155)
        goal_orientation = (1.0, 0.0, 0.22)

        cameras_kwargs = [
            {
                "name": "rgb",
                "postprocessing": "SceneFinal",
            },
            {
                "name": "depth",
                "postprocessing": "Depth",
            },
        ]
        lidars_kwargs = []
        carla_op_subscribers = []

        if FLAGS.carla_segmented:
            # Segmented camera. The stream comes from CARLA.
            segmented_video_op = graph.add(
                SegmentedVideoOperator, name='segmented_video')
            carla_op_subscribers.append(segmented_video_op)
            cameras_kwargs.append({
                "name": "segmented",
                "postprocessing": "SemanticSegmentation",
            })

        if FLAGS.lidar:
            lidar_visualizer_op = graph.add(
                LidarVisualizerOperator, name='lidar_visualizer')
            carla_op_subscribers.append(lidar_visualizer_op)
            lidars_kwargs.append({"name": "lidar"})
            if FLAGS.record_lidar:
                record_lidar_op = graph.add(
                    RecordOp,
                    name='record_lidar',
                    init_args={'filename': 'pylot_lidar_data.erdos'},
                    setup_args={'filter': 'lidar'})
                carla_op_subscribers.append(record_lidar_op)

        # Define ops
        carla_op = graph.add(
            CarlaOperator,
            name='carla',
            init_args={
                'host': FLAGS.carla_host,
                'port': FLAGS.carla_port,
                'num_vehicles': FLAGS.carla_num_vehicles,
                'num_pedestrians': FLAGS.carla_num_pedestrians
            })
        control_op = graph.add(ControlOperator, name='control')
        agent_op = graph.add(
            AgentOperator,
            name='agent',
            init_args={
                'city_name': 'Town01',
                'goal_location': goal_location,
                'goal_orientation': goal_orientation
            })
        carla_to_image_op = graph.add(
            CarlaToImageOperator,
            name='rgb_images',
            setup_args={
                "op_name": "rgb_images",
                "filter_name": "rgb"
            })
        camera_ops = [carla_to_image_op]

        # Connect ops
        graph.connect([carla_op], carla_op_subscribers)
        graph.connect([carla_op], [carla_to_image_op])
        graph.connect([carla_op], [agent_op])
        graph.connect([agent_op], [carla_op])

        # Add and connect ops based on conditions
        if FLAGS.depth_camera_video:
            depth_video_op = graph.add(
                VideoOperator,
                name='depth_camera_video',
                setup_args={"filter_name": "depth"})
            graph.connect([carla_op], [depth_video_op])

        if FLAGS.record_rgb_camera:
            record_rgb_op = graph.add(
                RecordOp,
                name='record_rgb_camera',
                init_args={'filename': 'pylot_rgb_camera_data.erdos'},
                setup_args={'filter': 'rgb'})
            graph.connect([carla_op], [record_rgb_op])

        if FLAGS.record_depth_camera:
            record_depth_camera_op = graph.add(
                RecordOp,
                name='record_depth_camera',
                init_args={'filename': 'pylot_depth_camera_data.erdos'},
                setup_args={'filter': 'depth'})
            graph.connect([carla_op], [record_depth_camera_op])

        if FLAGS.record_ground_truth:
            input_names = [
                'vehicle_pos', 'acceleration', 'forward_speed',
                'traffic_lights', 'pedestrians', 'vehicles', 'traffic_signs'
            ]
            record_carla_op = graph.add(
                RecordOp,
                name='record_carla',
                init_args={'filename': 'pylot_carla_data.erdos'},
                setup_args={'filter': input_names})
            graph.connect([carla_op], [record_carla_op])

    # XXX(ionel): This planner is not currently in use.
    # planner_op = PlannerOperator('Town01', goal_location, goal_orientation)
    # planner_streams = planner_op([carla_op.get_output_stream('vehicle_pos')])
    # control_streams = control_op(planner_streams)

    if FLAGS.rgb_camera_video:
        # FIXME(yika): VideoOperator only process Carla images, so camera replay will fail here
        camera_video_op = graph.add(
            VideoOperator,
            name='rgb_camera',
            setup_args={"filter_name": "rgb"})
        graph.connect([carla_op], [camera_video_op])

    if FLAGS.segmentation:
        # FIXME(yika): segmentation op implementation problem (see SegmentationOperator)
        segmentation_op = graph.add(SegmentationOperator, name='segmentation')
        graph.connect(camera_ops, [segmentation_op])

    if FLAGS.obj_detection:
        obj_detector_op = graph.add(DetectionOperator, name='detection')
        graph.connect(camera_ops, [obj_detector_op])

        if FLAGS.obj_tracking:
            tracker_op = graph.add(
                ObjectTrackerOperator, name='object_tracker')
            graph.connect([obj_detector_op], [tracker_op])

        if FLAGS.fusion and not FLAGS.replay:
            fusion_op = graph.add(FusionOperator, name='fusion')
            fusion_verification_op = graph.add(
                FusionVerificationOperator, name='fusion_verifier')
            graph.connect([obj_detector_op, obj_detector_op, carla_op],
                          [fusion_op])
            graph.connect([fusion_op, carla_op], [fusion_verification_op])

    if FLAGS.traffic_light_det:
        traffic_light_det_op = graph.add(
            TrafficLightDetOperator, name='traffic_light_detector')
        graph.connect(camera_ops, [traffic_light_det_op])

    graph.execute(FLAGS.framework)


if __name__ == '__main__':
    app.run(main)
