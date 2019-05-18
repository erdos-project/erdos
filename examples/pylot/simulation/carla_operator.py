import numpy as np
from std_msgs.msg import Float64
import time
import ray

from carla.client import CarlaClient
from carla.image_converter import depth_to_array, labels_to_array, to_bgra_array
from carla.sensor import Camera, Lidar
from carla.settings import CarlaSettings
from carla.transform import Transform

from erdos.data_stream import DataStream
from erdos.message import Message, WatermarkMessage
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_csv_logging, setup_logging, time_epoch_ms

from perception.messages import SegmentedFrameMessage
import pylot_utils
import simulation.messages
import simulation.utils


class CarlaOperator(Op):
    """Provides an ERDOS interface to the CARLA simulator.

    Args:
        synchronous_mode (bool): whether the simulator will wait for control
            input from the client.
    """
    def __init__(self,
                 name,
                 flags,
                 camera_setups=[],
                 lidar_stream_names=[],
                 log_file_name=None,
                 csv_file_name=None):
        super(CarlaOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
        self._csv_logger = setup_csv_logging(self.name + '-csv', csv_file_name)
        self.message_num = 0
        if self._flags.carla_high_quality:
            quality = 'Epic'
        else:
            quality = 'Low'
        self.settings = CarlaSettings()
        self.settings.set(
            SynchronousMode=self._flags.carla_synchronous_mode,
            SendNonPlayerAgentsInfo=True,
            NumberOfVehicles=self._flags.carla_num_vehicles,
            NumberOfPedestrians=self._flags.carla_num_pedestrians,
            WeatherId=self._flags.carla_weather,
            QualityLevel=quality)
        self.settings.randomize_seeds()
        self.lidar_streams = []
        self._transforms = {}
        for (camera_stream_name, camera_type, image_size, pos) in camera_setups:
            transform = self.__add_camera(name=camera_stream_name,
                                          postprocessing=camera_type,
                                          image_size=image_size,
                                          position=pos)
            self._transforms[camera_stream_name] = transform
        for lidar_stream_name in lidar_stream_names:
            self.__add_lidar(name=lidar_stream_name)
        self.agent_id_map = {}
        self.pedestrian_count = 0

    @staticmethod
    def setup_streams(input_streams, camera_setups, lidar_stream_names):
        input_streams.add_callback(CarlaOperator.update_control)
        camera_streams = [DataStream(name=camera,
                                     labels={'sensor_type': 'camera',
                                             'camera_type': camera_type})
                          for (camera, camera_type, _, _) in camera_setups]
        lidar_streams = [DataStream(name=lidar,
                                    labels={'sensor_type': 'lidar'})
                         for lidar in lidar_stream_names]
        return [
            DataStream(name='world_transform'),
            DataStream(name='vehicle_pos'),
            DataStream(name='acceleration'),
            DataStream(data_type=Float64, name='forward_speed'),
            DataStream(name='traffic_lights'),
            DataStream(name='pedestrians'),
            DataStream(name='vehicles'),
            DataStream(name='traffic_signs'),
        ] + camera_streams + lidar_streams

    def __add_camera(self,
                     name,
                     postprocessing,
                     image_size=(800, 600),
                     field_of_view=90.0,
                     position=(0.3, 0, 1.3),
                     rotation_pitch=0,
                     rotation_roll=0,
                     rotation_yaw=0):
        """Adds a camera and a corresponding output stream.

        Args:
            name: A string naming the camera.
            postprocessing: "SceneFinal", "Depth", "SemanticSegmentation".
        """
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

        self.settings.add_sensor(camera)
        return camera.get_unreal_transform()

    def __add_lidar(self,
                    name,
                    channels=32,
                    max_range=50,
                    points_per_second=100000,
                    rotation_frequency=10,
                    upper_fov_limit=10,
                    lower_fov_limit=-30,
                    position=(0, 0, 1.4),
                    rotation_pitch=0,
                    rotation_yaw=0,
                    rotation_roll=0):
        """Adds a LIDAR sensor and a corresponding output stream.

        Args:
            name: A string naming the camera.
        """
        lidar = Lidar(
            name,
            Channels=channels,
            Range=max_range,
            PointsPerSecond=points_per_second,
            RotationFrequency=rotation_frequency,
            UpperFovLimit=upper_fov_limit,
            LowerFovLimit=lower_fov_limit,
            PositionX=position[0],
            PositionY=position[1],
            PositionZ=position[2],
            RotationPitch=rotation_pitch,
            RotationYaw=rotation_yaw,
            RotationRoll=rotation_roll)

        self.settings.add_sensor(lidar)
        output_stream = DataStream(name=name, labels={"sensor_type": "lidar"})
        self.lidar_streams.append(output_stream)


    def read_carla_data(self):
        read_start_time = time.time()
        measurements, sensor_data = self.client.read_data()
        measure_time = time.time()

        self._logger.info(
            'Got readings for game time {} and platform time {}'.format(
                measurements.game_timestamp, measurements.platform_timestamp))

        # Send measurements
        player_measurements = measurements.player_measurements
        vehicle_pos = ((player_measurements.transform.location.x,
                        player_measurements.transform.location.y,
                        player_measurements.transform.location.z),
                       (player_measurements.transform.orientation.x,
                        player_measurements.transform.orientation.y,
                        player_measurements.transform.orientation.z))

        world_transform = Transform(player_measurements.transform)

        timestamp = Timestamp(
            coordinates=[measurements.game_timestamp, self.message_num])
        self.message_num += 1
        ray.register_custom_serializer(Message, use_pickle=True)
        ray.register_custom_serializer(WatermarkMessage, use_pickle=True)
        watermark = WatermarkMessage(timestamp)
        self.get_output_stream('world_transform').send(
            Message(world_transform, timestamp))
        self.get_output_stream('world_transform').send(watermark)
        self.get_output_stream('vehicle_pos').send(
            Message(vehicle_pos, timestamp))
        self.get_output_stream('vehicle_pos').send(watermark)
        acceleration = (player_measurements.acceleration.x,
                        player_measurements.acceleration.y,
                        player_measurements.acceleration.z)
        self.get_output_stream('acceleration').send(
            Message(acceleration, timestamp))
        self.get_output_stream('acceleration').send(watermark)
        self.get_output_stream('forward_speed').send(
            Message(player_measurements.forward_speed, timestamp))
        self.get_output_stream('forward_speed').send(watermark)

        vehicles = []
        pedestrians = []
        traffic_lights = []
        speed_limit_signs = []

        for agent in measurements.non_player_agents:
            if agent.HasField('vehicle'):
                pos = simulation.utils.Transform(agent.vehicle.transform)
                bb = simulation.utils.BoundingBox(agent.vehicle.bounding_box)
                forward_speed = agent.vehicle.forward_speed
                vehicle = simulation.messages.Vehicle(pos, bb, forward_speed)
                vehicles.append(vehicle)
            elif agent.HasField('pedestrian'):
                if not self.agent_id_map.get(agent.id):
                    self.pedestrian_count += 1
                    self.agent_id_map[agent.id] = self.pedestrian_count

                pedestrian_index = self.agent_id_map[agent.id]
                pos = simulation.utils.Transform(agent.pedestrian.transform)
                bb = simulation.utils.BoundingBox(agent.pedestrian.bounding_box)
                forward_speed = agent.pedestrian.forward_speed
                pedestrian = simulation.messages.Pedestrian(
                    pedestrian_index, pos, bb, forward_speed)
                pedestrians.append(pedestrian)
            elif agent.HasField('traffic_light'):
                transform = simulation.utils.Transform(
                    agent.traffic_light.transform)
                traffic_light = simulation.messages.TrafficLight(
                    transform, agent.traffic_light.state)
                traffic_lights.append(traffic_light)
            elif agent.HasField('speed_limit_sign'):
                transform = simulation.utils.Transform(
                    agent.speed_limit_sign.transform)
                speed_sign = simulation.messages.SpeedLimitSign(
                    transform, agent.speed_limit_sign.speed_limit)
                speed_limit_signs.append(speed_sign)

        vehicles_msg = simulation.messages.GroundVehiclesMessage(
            vehicles, timestamp)
        self.get_output_stream('vehicles').send(vehicles_msg)
        self.get_output_stream('vehicles').send(watermark)
        pedestrians_msg = simulation.messages.GroundPedestriansMessage(
            pedestrians, timestamp)
        self.get_output_stream('pedestrians').send(pedestrians_msg)
        self.get_output_stream('pedestrians').send(watermark)
        traffic_lights_msg = simulation.messages.GroundTrafficLightsMessage(
            traffic_lights, timestamp)
        self.get_output_stream('traffic_lights').send(traffic_lights_msg)
        self.get_output_stream('traffic_lights').send(watermark)
        traffic_sings_msg = simulation.messages.GroundSpeedSignsMessage(
            speed_limit_signs, timestamp)
        self.get_output_stream('traffic_signs').send(traffic_sings_msg)
        self.get_output_stream('traffic_signs').send(watermark)

        # Send sensor data
        for name, measurement in sensor_data.items():
            data_stream = self.get_output_stream(name)
            if data_stream.get_label('camera_type') == 'SceneFinal':
                # Transform the Carla RGB images to BGR.
                data_stream.send(
                    simulation.messages.FrameMessage(
                        pylot_utils.bgra_to_bgr(to_bgra_array(measurement)), timestamp))
            elif data_stream.get_label('camera_type') == 'SemanticSegmentation':
                frame = labels_to_array(measurement)
                data_stream.send(SegmentedFrameMessage(frame, 0, timestamp))
            elif data_stream.get_label('camera_type') == 'Depth':
                # NOTE: depth_to_array flips the image.
                data_stream.send(
                    simulation.messages.DepthFrameMessage(
                        depth_to_array(measurement),
                        self._transforms[name],
                        measurement.fov,
                        timestamp))
            else:
                data_stream.send(Message(measurement, timestamp))
            data_stream.send(watermark)

        self.client.send_control(**self.control)
        end_time = time.time()

        measurement_runtime = (measure_time - read_start_time) * 1000
        total_runtime = (end_time - read_start_time) * 1000
        self._logger.info('Carla measurement time {}; total time {}'.format(
            measurement_runtime, total_runtime))
        self._csv_logger.info('{},{},{},{}'.format(
            time_epoch_ms(), self.name, measurement_runtime, total_runtime))

    def read_data_at_frequency(self):
        period = 1.0 / self._flags.carla_step_frequency
        trigger_at = time.time() + period
        while True:
            time_until_trigger = trigger_at - time.time()
            if time_until_trigger > 0:
                time.sleep(time_until_trigger)
            else:
                self._logger.error('Cannot read Carla data at frequency {}'.format(
                    self._flags.carla_step_frequency))
            self.read_carla_data()
            trigger_at += period

    def update_control(self, msg):
        """Updates the control dict"""
        self.control['steer'] = msg.steer
        self.control['throttle'] = msg.throttle
        self.control['brake'] = msg.brake
        self.control['hand_brake'] = msg.hand_brake
        self.control['reverse'] = msg.reverse

    def execute(self):
        # Subscribe to control streams
        self.control = {
            'steer': 0.0,
            'throttle': 0.0,
            'brake': 0.0,
            'hand_brake': False,
            'reverse': False
        }
        self.client = CarlaClient(self._flags.carla_host,
                                  self._flags.carla_port,
                                  timeout=10)
        self.client.connect()
        scene = self.client.load_settings(self.settings)

        # Choose one player start at random.
        number_of_player_starts = len(scene.player_start_spots)
        player_start = self._flags.carla_start_player_num
        if self._flags.carla_random_player_start:
            player_start = np.random.randint(
                0, max(0, number_of_player_starts - 1))

        self.client.start_episode(player_start)

        self.read_data_at_frequency()
        self.spin()
