from erdos.message import Message


class WaypointMessage(Message):
    """ This class represents a message to be used to send waypoints."""

    def __init__(self, wp_angle, wp_vector, wp_angle_speed, wp_vector_speed,
                 timestamp, stream_name='default'):
        super(WaypointMessage, self).__init__(None, timestamp, stream_name)
        self.wp_angle = wp_angle
        self.wp_vector = wp_vector
        self.wp_angle_speed = wp_angle_speed
        self.wp_vector_speed = wp_vector_speed

    def __str__(self):
        return 'timestamp: {}, wp_angle: {}, wp_vector: {}, wp_angle_speed: {}, wp_vector_speed: {}'.format(
            self.timestamp, self.wp_angle, self.wp_vector, self.wp_angle_speed, self.wp_vector_speed)
