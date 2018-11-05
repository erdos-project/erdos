from collections import namedtuple


class Position(object):
    def __init__(self, transform):
        self.location = (transform.location.x, transform.location.y,
                         transform.location.z)
        self.orientation = (transform.orientation.x, transform.orientation.y)
        self.yaw = transform.rotation.yaw


class BoundingBox(object):
    def __init__(self, bb):
        self.location_z = bb.transform.location.z
        self.orientation_x = bb.transform.orientation.x
        self.extent = (bb.extent.x, bb.extent.y, bb.extent.z)


Vehicle = namedtuple('Vehicle', 'position, bounding_box, forward_speed')
Pedestrian = namedtuple('Pedestrian', 'position, bounding_box, forward_speed')
TrafficLight = namedtuple('TrafficLight', 'position, state')
SpeedLimitSign = namedtuple('SpeedLimitSign', 'position, limit')
