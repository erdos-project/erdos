from absl import flags
import cv2
import rospy
import scipy.misc

import carla

from srunner.challenge.autoagents.autonomous_agent import AutonomousAgent, Track


from erdos.data_stream import DataStream
import erdos.graph
from erdos.message import Message
from erdos.ros.ros_output_data_stream import ROSOutputDataStream
from erdos.timestamp import Timestamp

import config
from debug.video_operator import VideoOperator
#import operator_creator
from perception.segmentation.segmentation_drn_operator import SegmentationDRNOperator
import pylot_utils
import simulation.messages

FLAGS = flags.FLAGS
RGB_CAMERA_NAME='front_center_camera'


class ERDOSAgent(AutonomousAgent):
    def setup(self, path_to_conf_file):
        flags.FLAGS([__file__, '--flagfile={}'.format(path_to_conf_file)])
        self.track = Track.ALL_SENSORS_HDMAP_WAYPOINTS

        self.message_num = 0

        # Set up graph
        self.graph = erdos.graph.get_current_graph()
        self.camera_stream = ROSOutputDataStream(
            DataStream(name=RGB_CAMERA_NAME,
                       uid=RGB_CAMERA_NAME,
                       labels={'sensor_type': 'camera',
                               'camera_type': 'SceneFinal'}))

        # video_op = self.graph.add(
        #     VideoOperator,
        #     name='video_op',
        #     init_args={'flags': FLAGS,
        #                'log_file_name': FLAGS.log_file_name},
        #     setup_args={'filter_name': RGB_CAMERA_NAME},
        #     input_streams=[self.camera_stream])

        segmentation_op = self.graph.add(
            SegmentationDRNOperator,
            name='segmentation_drn',
            setup_args={'output_stream_name': 'segmented_stream'},
            init_args={'output_stream_name': 'segmented_stream',
                       'flags': FLAGS,
                       'log_file_name': FLAGS.log_file_name,
                       'csv_file_name': FLAGS.csv_log_file_name},
            _resources = {"GPU": FLAGS.segmentation_drn_gpu_memory_fraction},
            input_streams=[self.camera_stream])

        # Execute graph
        self.graph.execute(FLAGS.framework, blocking=False)

        rospy.init_node("erdos_driver", anonymous=True)

        self.camera_stream.setup()

    def sensors(self):
        """
        Define the sensor suite required by the agent

        :return: a list containing the required sensors in the following format:

        [
            {'type': 'sensor.camera.rgb', 'x': 0.7, 'y': -0.4, 'z': 1.60, 'roll': 0.0, 'pitch': 0.0, 'yaw': 0.0,
                      'width': 300, 'height': 200, 'fov': 100, 'id': 'Left'},

            {'type': 'sensor.camera.rgb', 'x': 0.7, 'y': 0.4, 'z': 1.60, 'roll': 0.0, 'pitch': 0.0, 'yaw': 0.0,
                      'width': 300, 'height': 200, 'fov': 100, 'id': 'Right'},

            {'type': 'sensor.lidar.ray_cast', 'x': 0.7, 'y': 0.0, 'z': 1.60, 'yaw': 0.0, 'pitch': 0.0, 'roll': 0.0,
             'id': 'LIDAR'}


        """
        # sensors = [{'type': 'sensor.camera.rgb', 'x': 0.7, 'y': 0.0, 'z': 1.60, 'roll':0.0, 'pitch':0.0, 'yaw': 0.0,
        #             'width': 800, 'height': 600, 'fov':100, 'id': 'Center'},
        #            {'type': 'sensor.camera.rgb', 'x': 0.7, 'y': -0.4, 'z': 1.60, 'roll': 0.0, 'pitch': 0.0,
        #             'yaw': -45.0, 'width': 800, 'height': 600, 'fov': 100, 'id': 'Left'},
        #            {'type': 'sensor.camera.rgb', 'x': 0.7, 'y': 0.4, 'z': 1.60, 'roll': 0.0, 'pitch': 0.0, 'yaw': 45.0,
        #             'width': 800, 'height': 600, 'fov': 100, 'id': 'Right'},
        #            {'type': 'sensor.lidar.ray_cast', 'x': 0.7, 'y': -0.4, 'z': 1.60, 'roll': 0.0, 'pitch': 0.0,
        #             'yaw': -45.0, 'id': 'LIDAR'},
        #            {'type': 'sensor.other.gnss', 'x': 0.7, 'y': -0.4, 'z': 1.60, 'id': 'GPS'},
        #            {'type': 'sensor.can_bus', 'reading_frequency': 25, 'id': 'can_bus'},
        #            {'type': 'sensor.hd_map', 'reading_frequency': 1, 'id': 'hdmap'},
        #           ]

        sensors = [{'type': 'sensor.camera.rgb', 'x': 0.7, 'y': 0.0, 'z': 1.60, 'roll':0.0, 'pitch':0.0, 'yaw': 0.0,
                    'width': 800, 'height': 600, 'fov':100, 'id': RGB_CAMERA_NAME}]
        return sensors

    def run_step(self, input_data, timestamp):
        erdos_timestamp = Timestamp(coordinates=[timestamp, self.message_num])
        self.message_num += 1
        for key, val in input_data.items():
            print("{} {} {}".format(key, val[0], type(val[1])))
            if key == RGB_CAMERA_NAME:
                self.camera_stream.send(
                    simulation.messages.FrameMessage(pylot_utils.bgra_to_bgr(val[1]),
                                                     erdos_timestamp))

        # DO SOMETHING SMART

        # RETURN CONTROL
        control = carla.VehicleControl()
        control.steer = 0.0
        control.throttle = 1.0
        control.brake = 0.0
        control.hand_brake = False

        return control
