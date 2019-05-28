from absl import flags
import pickle
import rospy
from std_msgs.msg import String
import scipy.misc
import time
import threading

import carla

from srunner.challenge.autoagents.autonomous_agent import AutonomousAgent, Track

from erdos.data_stream import DataStream
import erdos.graph
from erdos.message import Message, WatermarkMessage
from erdos.operators import NoopOp
from erdos.ros.ros_output_data_stream import ROSOutputDataStream
from erdos.timestamp import Timestamp

import config
from control.pid_control_operator import PIDControlOperator
from debug.video_operator import VideoOperator
import operator_creator
from perception.segmentation.segmentation_drn_operator import SegmentationDRNOperator
from planning.planning_operator import PlanningOperator
import pylot_utils
import simulation.messages
import simulation.utils


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


def create_planning_op(graph):
    planning_op = graph.add(
        PlanningOperator,
        name='planning',
        init_args={
            'flags': FLAGS,
            'log_file_name': FLAGS.log_file_name,
            'csv_file_name': FLAGS.csv_log_file_name
        })
    return planning_op


def create_control_op(graph):
    control_op = graph.add(
        PIDControlOperator,
        name='controller',
        init_args={
            'longitudinal_control_args': {
                'K_P': FLAGS.pid_p,
                'K_I': FLAGS.pid_i,
                'K_D': FLAGS.pid_d,
            },
            'flags': FLAGS,
            'log_file_name': FLAGS.log_file_name,
            'csv_file_name': FLAGS.csv_log_file_name
        })
    return control_op


class ERDOSAgent(AutonomousAgent):

    def setup(self, path_to_conf_file):
        flags.FLAGS([__file__, '--flagfile={}'.format(path_to_conf_file)])
        self.track = Track.ALL_SENSORS_HDMAP_WAYPOINTS

        self._lock = threading.Lock()
        self._vehicle_transform = None
        self._waypoints = None
        self._sent_open_drive_data = False
        self._open_drive_data = None
        self._message_num = 0

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

        planning_ops = [create_planning_op(self.graph)]

        control_op = create_control_op(self.graph)

        self.graph.connect(
            [scenario_input_op],
            segmentation_ops + obj_detector_ops + tracker_ops +
            traffic_light_det_ops + lane_detection_ops + planning_ops +
            visualization_ops + [control_op])

        self.graph.connect(planning_ops, [control_op])

        # Execute graph
        self.graph.execute(FLAGS.framework, blocking=False)

        rospy.init_node("erdos_driver", anonymous=True)
        # Subscribe to the control stream
        rospy.Subscriber('default/controller/control_stream',
                         String,
                         callback=self.on_control_msg,
                         queue_size=None)

        self._camera_stream.setup()
        self._global_trajectory_stream.setup()
        self._open_drive_stream.setup()
        self._vehicle_transform_stream.setup()
        self._can_bus_stream.setup()

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
                       'reading_frequency': 60,
                       'id': 'can_bus'}]
        gps_sensor = [{'type': 'sensor.other.gnss',
                       'x': 0.7,
                       'y': -0.4,
                       'z': 1.60,
                       'id': 'GPS'}]
        hd_map_sensor = [{'type': 'sensor.hd_map',
                          'reading_frequency': 30,
                          'id': 'hdmap'}]
        front_camera_sensor = [{'type': 'sensor.camera.rgb',
                                'x': 2.0,
                                'y': 0.0,
                                'z': 1.40,
                                'roll':0.0,
                                'pitch':0.0,
                                'yaw': 0.0,
                                'width': 800,
                                'height': 600,
                                'fov':100,
                                'id': RGB_CAMERA_NAME}]
        # TODO(ionel): Put the Lidar in the same location as the camera.
        lidar_sensor = [{'type': 'sensor.lidar.ray_cast',
                         'x': 0.7,
                         'y': -0.4,
                         'z': 1.60,
                         'roll': 0.0,
                         'pitch': 0.0,
                         'yaw': -45.0,
                         'id': 'LIDAR'}]
        sensors = can_sensor + gps_sensor + hd_map_sensor + front_camera_sensor + lidar_sensor
        return sensors

    def run_step(self, input_data, timestamp):
        with self._lock:
            self._control = None
        erdos_timestamp = Timestamp(coordinates=[timestamp, self._message_num])
        watermark = WatermarkMessage(erdos_timestamp)
        self._message_num += 1

        # Send once the global waypoints.
        if self._waypoints is None:
            self._waypoints = self._global_plan_world_coord
            data = [(simulation.utils.to_erdos_transform(transform), road_option)
                    for (transform, road_option) in self._waypoints]
            self._global_trajectory_stream.send(Message(data, erdos_timestamp))
            self._global_trajectory_stream.send(watermark)
        assert self._waypoints == self._global_plan_world_coord,\
            'Global plan has been updated.'

        for key, val in input_data.items():
            #print("{} {} {}".format(key, val[0], type(val[1])))
            if key == RGB_CAMERA_NAME:
                self._camera_stream.send(
                    simulation.messages.FrameMessage(pylot_utils.bgra_to_bgr(val[1]),
                                                     erdos_timestamp))
                self._camera_stream.send(watermark)
            elif key == 'can_bus':
                can_bus = simulation.utils.CanBus(**val[1])
                self._can_bus_stream.send(Message(can_bus, erdos_timestamp))
                self._can_bus_stream.send(watermark)
            elif key == 'GPS':
                gps = simulation.utils.LocationGeo(val[1][0], val[1][1], val[1][2])
            elif key == 'hdmap':
                # Sending once opendrive data
                if not self._sent_open_drive_data:
                    self._open_drive_data = val[1]['opendrive']
                    self._sent_open_drive_data = True
                    self._open_drive_stream.send(
                        Message(self._open_drive_data, erdos_timestamp))
                    self._open_drive_stream.send(watermark)
                assert self._open_drive_data == val[1]['opendrive'],\
                    'Opendrive data changed.'

                # Sending vehicle transform data.
                vec_trans_dict = val[1]['transform']
                loc = simulation.utils.Location(
                    vec_trans_dict['x'],
                    vec_trans_dict['y'],
                    vec_trans_dict['z'])
                self._vehicle_transform = simulation.utils.Transform(
                    loc,
                    vec_trans_dict['pitch'],
                    vec_trans_dict['yaw'],
                    vec_trans_dict['roll'])
                self._vehicle_transform_stream.send(
                    Message(self._vehicle_transform, erdos_timestamp))
                self._vehicle_transform_stream.send(watermark)

                # TODO(ionel): Send point cloud data.
                pc_file = val[1]['map_file']
            elif key == 'LIDAR':
                # TODO(ionel): Send point cloud data.
                pass

        # Wait until the control is set.
        while self._control is None:
            time.sleep(0.01)

        return self._control

    def on_control_msg(self, msg):
        msg = pickle.loads(msg.data)
        if not isinstance(msg, WatermarkMessage):
            with self._lock:
                print("Received control message {}".format(msg))
                self._control = carla.VehicleControl()
                self._control.throttle = msg.throttle
                self._control.brake = msg.brake
                self._control.steer = msg.steer
                self._control.reverse = msg.reverse
                self._control.hand_brake = msg.hand_brake
                self._control.manual_gear_shift = False

    def __create_scenario_input_op(self):
        self._camera_stream = ROSOutputDataStream(
            DataStream(name=RGB_CAMERA_NAME,
                       uid=RGB_CAMERA_NAME,
                       labels={'sensor_type': 'camera',
                               'camera_type': 'sensor.camera.rgb'}))

        self._vehicle_transform_stream = ROSOutputDataStream(
            DataStream(name='vehicle_transform',
                       uid='vehicle_transform'))

        # Stream on which we send the global trajectory.
        self._global_trajectory_stream = ROSOutputDataStream(
            DataStream(name='global_trajectory_stream',
                       uid='global_trajectory_stream',
                       labels={'global': 'true',
                               'waypoints': 'true'}))

        # Stream on which we send the opendrive map.
        self._open_drive_stream = ROSOutputDataStream(
            DataStream(name='open_drive_stream',
                       uid='open_drive_stream'))

        self._can_bus_stream = ROSOutputDataStream(
            DataStream(name='can_bus', uid='can_bus'))

        return self.graph.add(NoopOp,
                              name='scenario_input',
                              input_streams=[self._camera_stream,
                                             self._vehicle_transform_stream,
                                             self._global_trajectory_stream,
                                             self._open_drive_stream,
                                             self._can_bus_stream])
