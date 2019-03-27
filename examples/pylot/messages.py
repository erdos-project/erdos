from collections import namedtuple
import carla.carla_server_pb2

# We are transforming protobuf objects to python objects and
# back because we cannot pickle protobuf objects.
class Transform(object):
    def __init__(self, transform):
        self.location = (transform.location.x, transform.location.y,
                         transform.location.z)
        self.orientation = (transform.orientation.x,
                            transform.orientation.y,
                            transform.orientation.z)
        self.rotation = (transform.rotation.pitch,
                         transform.rotation.yaw,
                         transform.rotation.roll)

    def populate_pb2(self, transform):
        transform.location.x = self.location[0]
        transform.location.y = self.location[1]
        transform.location.z = self.location[2]
        transform.orientation.x = self.orientation[0]
        transform.orientation.y = self.orientation[1]
        transform.orientation.z = self.orientation[2]
        transform.rotation.pitch = self.rotation[0]
        transform.rotation.yaw = self.rotation[1]
        transform.rotation.roll = self.rotation[2]

    def to_transform_pb2(self):
        transform = carla.carla_server_pb2.Transform()
        self.populate_pb2(transform)
        return transform

class BoundingBox(object):
    def __init__(self, bb):
        self.transform = Transform(bb.transform)
        self.extent = (bb.extent.x, bb.extent.y, bb.extent.z)

    def to_bounding_box_pb2(self):
        bb = carla.carla_server_pb2.BoundingBox()
        self.transform.populate_pb2(bb.transform)
        bb.extent.x = self.extent[0]
        bb.extent.y = self.extent[1]
        bb.extent.z = self.extent[2]
        return bb

Vehicle = namedtuple('Vehicle', 'position, bounding_box, forward_speed')
Pedestrian = namedtuple('Pedestrian', 'position, bounding_box, forward_speed')
TrafficLight = namedtuple('TrafficLight', 'position, state')
SpeedLimitSign = namedtuple('SpeedLimitSign', 'position, limit')
