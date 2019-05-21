from absl import flags
import cv2
import rospy
import scipy.misc

import carla

from srunner.challenge.autoagents.autonomous_agent import AutonomousAgent, Track


from erdos.data_stream import DataStream
import erdos.graph
from erdos.message import Message
from erdos.operators import NoopOp
from erdos.ros.ros_output_data_stream import ROSOutputDataStream
from erdos.timestamp import Timestamp

import config
from debug.video_operator import VideoOperator
import operator_creator
from perception.segmentation.segmentation_drn_operator import SegmentationDRNOperator
import pylot_utils
import simulation.messages

FLAGS = flags.FLAGS
RGB_CAMERA_NAME='front_center_camera'


def add_visualization_operators(graph, rgb_camera_name):
    visualization_ops = []
    if FLAGS.visualize_rgb_camera:
        camera_video_op = operator_creator.create_camera_video_op(
            graph, 'rgb_camera', rgb_camera_name)
        visualization_ops.append(camera_video_op)
    if FLAGS.visualize_segmentation:
        # Segmented camera. The stream comes from CARLA.
        segmented_video_op = operator_creator.create_segmented_video_op(graph)
        visualization_ops.append(segmented_video_op)
    return visualization_ops


class ERDOSAgent(AutonomousAgent):

    def setup(self, path_to_conf_file):
        flags.FLAGS([__file__, '--flagfile={}'.format(path_to_conf_file)])
        self.track = Track.ALL_SENSORS_HDMAP_WAYPOINTS

        self.message_num = 0

        # Set up graph
        self.graph = erdos.graph.get_current_graph()

        scenario_input_op = self.__create_scenario_input_op()

        visualization_ops = add_visualization_operators(self.graph, RGB_CAMERA_NAME)

        segmentation_ops = []
        if FLAGS.segmentation_drn:
            segmentation_op = operator_creator.create_segmentation_drn_op(self.graph)
            segmentation_ops.append(segmentation_op)

        if FLAGS.segmentation_dla:
            segmentation_op = operator_creator.create_segmentation_dla_op(self.graph)
            segmentation_ops.append(segmentation_op)

        obj_detector_ops = []
        tracker_ops = []
        if FLAGS.obj_detection:
            obj_detector_ops = operator_creator.create_detector_ops(self.graph)
            if FLAGS.obj_tracking:
                tracker_op = operator_creator.create_object_tracking_op(self.graph)
                tracker_ops.append(tracker_op)

        traffic_light_det_ops = []
        if FLAGS.traffic_light_det:
            traffic_light_det_ops.append(operator_creator.create_traffic_light_op(self.graph))

        lane_detection_ops = []
        if FLAGS.lane_detection:
            lane_detection_ops.append(operator_creator.create_lane_detection_op(self.graph))

        self.graph.connect(
            [scenario_input_op],
            segmentation_ops + obj_detector_ops + tracker_ops +
            traffic_light_det_ops + lane_detection_ops + visualization_ops)

        # Execute graph
        self.graph.execute(FLAGS.framework, blocking=False)

        rospy.init_node("erdos_driver", anonymous=True)

        self.camera_stream.setup()

    def sensors(self):
        """
        Define the sensor suite required by the agent.
        """
        # sensors = [{'type': 'sensor.camera.rgb', 'x': 0.7, 'y': -0.4, 'z': 1.60, 'roll': 0.0, 'pitch': 0.0,
        #             'yaw': -45.0, 'width': 800, 'height': 600, 'fov': 100, 'id': 'Left'},
        #            {'type': 'sensor.camera.rgb', 'x': 0.7, 'y': 0.4, 'z': 1.60, 'roll': 0.0, 'pitch': 0.0, 'yaw': 45.0,
        #             'width': 800, 'height': 600, 'fov': 100, 'id': 'Right'},
        #           ]

        can_sensor = [{'type': 'sensor.can_bus',
                       'reading_frequency': 25,
                       'id': 'can_bus'}]
        gps_sensor = [{'type': 'sensor.other.gnss',
                       'x': 0.7,
                       'y': -0.4,
                       'z': 1.60,
                       'id': 'GPS'}]
        hd_map_sensor = [{'type': 'sensor.hd_map',
                          'reading_frequency': 1,
                          'id': 'hdmap'}]
        front_camera_sensor = [{'type': 'sensor.camera.rgb',
                                'x': 0.7,
                                'y': 0.0,
                                'z': 1.60,
                                'roll':0.0,
                                'pitch':0.0,
                                'yaw': 0.0,
                                'width': 800,
                                'height': 600,
                                'fov':100,
                                'id': RGB_CAMERA_NAME}]
        # lidar_sensor = [{'type': 'sensor.lidar.ray_cast',
        #                  'x': 0.7,
        #                  'y': -0.4,
        #                  'z': 1.60,
        #                  'roll': 0.0,
        #                  'pitch': 0.0,
        #                  'yaw': -45.0,
        #                  'id': 'LIDAR'}]
        sensors = can_sensor + gps_sensor + hd_map_sensor + front_camera_sensor
        return sensors


# self._global_plan is [({'lat': 49.001610853132775, 'z': 0.0, 'lon': 7.999952677036802}, <RoadOption.LANEFOLLOW: 4>), ...]

    def run_step(self, input_data, timestamp):
        erdos_timestamp = Timestamp(coordinates=[timestamp, self.message_num])
        self.message_num += 1
        for key, val in input_data.items():
            print("{} {} {}".format(key, val[0], type(val[1])))
            if key == RGB_CAMERA_NAME:
                self.camera_stream.send(
                    simulation.messages.FrameMessage(pylot_utils.bgra_to_bgr(val[1]),
                                                     erdos_timestamp))
            elif key == 'can_bus':
                can_bus = simulation.messages.CanBus(**val[1])
            elif key == 'GPS':
                gps = simulation.messages.LocationGeo(val[1][0], val[1][1], val[1][2])
            elif key == 'hdmap':
                opendrive = val[1]['opendrive']
                map = carla.Map('test', opendrive)
                transform = val[1]['transform']
                pc_file = val[1]['map_file']


        # DO SOMETHING SMART

        # RETURN CONTROL
        control = carla.VehicleControl()
        control.steer = 0.0
        control.throttle = 1.0
        control.brake = 0.0
        control.hand_brake = False

        return control


    def __create_scenario_input_op(self):
        self.camera_stream = ROSOutputDataStream(
            DataStream(name=RGB_CAMERA_NAME,
                       uid=RGB_CAMERA_NAME,
                       labels={'sensor_type': 'camera',
                               'camera_type': 'SceneFinal'}))

        return self.graph.add(NoopOp,
                              name='scenario_input',
                              input_streams=[self.camera_stream])
