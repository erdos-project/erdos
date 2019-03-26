import numpy as np
from std_msgs.msg import Float64

from carla.client import CarlaClient
from carla.sensor import Camera, Lidar
from carla.settings import CarlaSettings
from carla.transform import Transform

from erdos.data_stream import DataStream
from erdos.message import Message
from erdos.op import Op
from erdos.timestamp import Timestamp
from erdos.utils import frequency, setup_logging

import messages


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
                 log_file_name=None):
        super(CarlaOperator, self).__init__(name)
        self._flags = flags
        self._logger = setup_logging(self.name, log_file_name)
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
        for (camera_stream_name, camera_type) in camera_setups:
            self.__add_camera(name=camera_stream_name,
                              postprocessing=camera_type)
        for lidar_stream_name in lidar_stream_names:
            self.__add_lidar(name=lidar_stream_name)

    @staticmethod
    def setup_streams(input_streams, camera_setups, lidar_stream_names):
        input_streams.add_callback(CarlaOperator.update_control)
        camera_streams = [DataStream(name=camera,
                                     labels={'sensor_type': 'camera',
                                             'camera_type': camera_type})
                          for (camera, camera_type) in camera_setups]
        lidar_streams = [DataStream(name=lidar)
                         for lidar in lidar_stream_names]
        return [
            DataStream(name='world_transform'),
            DataStream(name='vehicle_pos'),
            DataStream(name='acceleration'),
            DataStream(data_type=Float64, name='forward_speed'),
            DataStream(data_type=Float64, name='vehicle_collisions'),
            DataStream(data_type=Float64, name='pedestrian_collisions'),
            DataStream(data_type=Float64, name='other_collisions'),
            DataStream(data_type=Float64, name='other_lane'),
            DataStream(data_type=Float64, name='offroad'),
            DataStream(name='traffic_lights'),
            DataStream(name='pedestrians'),
            DataStream(name='vehicles'),
            DataStream(name='traffic_signs'),
        ] + camera_streams + lidar_streams

    def __add_camera(self,
                     name,
                     postprocessing,
                     field_of_view=90.0,
                     image_size=(800, 600),
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

    # TODO(ionel): Set the frequency programmatically.
    @frequency(10)
    def step(self):
        measurements, sensor_data = self.client.read_data()

        # Send measurements
        player_measurements = measurements.player_measurements
        vehicle_pos = ((player_measurements.transform.location.x,
                        player_measurements.transform.location.y,
                        player_measurements.transform.location.z),
                       (player_measurements.transform.orientation.x,
                        player_measurements.transform.orientation.y,
                        player_measurements.transform.orientation.z))

        world_transform = Transform(player_measurements.transform)

        timestamp = Timestamp(coordinates=[self.message_num])
        self.message_num += 1
        self.get_output_stream('world_transform').send(
            Message(world_transform, timestamp))
        self.get_output_stream('vehicle_pos').send(
            Message(vehicle_pos, timestamp))
        acceleration = (player_measurements.acceleration.x,
                        player_measurements.acceleration.y,
                        player_measurements.acceleration.z)
        self.get_output_stream('acceleration').send(
            Message(acceleration, timestamp))
        self.get_output_stream('forward_speed').send(
            Message(player_measurements.forward_speed, timestamp))
        self.get_output_stream('vehicle_collisions').send(
            Message(player_measurements.collision_vehicles, timestamp))
        self.get_output_stream('pedestrian_collisions').send(
            Message(player_measurements.collision_pedestrians, timestamp))
        self.get_output_stream('other_collisions').send(
            Message(player_measurements.collision_other, timestamp))
        self.get_output_stream('other_lane').send(
            Message(player_measurements.intersection_otherlane, timestamp))
        self.get_output_stream('offroad').send(
            Message(player_measurements.intersection_offroad, timestamp))

        vehicles = []
        pedestrians = []
        traffic_lights = []
        speed_limit_signs = []

        for agent in measurements.non_player_agents:
            if agent.HasField('vehicle'):
                pos = messages.Transform(agent.vehicle.transform)
                bb = messages.BoundingBox(agent.vehicle.bounding_box)
                forward_speed = agent.vehicle.forward_speed
                vehicle = messages.Vehicle(pos, bb, forward_speed)
                vehicles.append(vehicle)
            elif agent.HasField('pedestrian'):
                pos = messages.Transform(agent.pedestrian.transform)
                bb = messages.BoundingBox(agent.pedestrian.bounding_box)
                forward_speed = agent.pedestrian.forward_speed
                pedestrian = messages.Pedestrian(pos, bb, forward_speed)
                pedestrians.append(pedestrian)
            elif agent.HasField('traffic_light'):
                transform = messages.Transform(agent.traffic_light.transform)
                traffic_light = messages.TrafficLight(
                    transform, agent.traffic_light.state)
                traffic_lights.append(traffic_light)
            elif agent.HasField('speed_limit_sign'):
                transform = messages.Transform(agent.speed_limit_sign.transform)
                speed_sign = messages.SpeedLimitSign(
                    transform, agent.speed_limit_sign.speed_limit)
                speed_limit_signs.append(speed_sign)

        vehicles_msg = Message(vehicles, timestamp)
        self.get_output_stream('vehicles').send(vehicles_msg)
        pedestrians_msg = Message(pedestrians, timestamp)
        self.get_output_stream('pedestrians').send(pedestrians_msg)
        traffic_lights_msg = Message(traffic_lights, timestamp)
        self.get_output_stream('traffic_lights').send(traffic_lights_msg)
        traffic_sings_msg = Message(speed_limit_signs, timestamp)
        self.get_output_stream('traffic_signs').send(traffic_sings_msg)

        # Send sensor data
        for name, measurement in sensor_data.items():
            self.get_output_stream(name).send(Message(measurement, timestamp))

        self.client.send_control(**self.control)

    def update_control(self, msg):
        """Updates the control dict"""
        self.control.update(msg.data)

    def execute(self):
        # Subscribe to control streams
        self.control = {
            'steer': 0.0,
            'throttle': 0.0,
            'break': 0.0,
            'hand_break': False,
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

        self.step()
        self.spin()
