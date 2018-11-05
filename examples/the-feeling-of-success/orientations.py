from geometry_msgs.msg import (
    PoseStamped,
    Pose,
    Point,
    Quaternion,
)


class Orientations:
    #x, y, z define axis, w defines how much rotation around that axis
    FORWARD_POINT = Quaternion(
        x=0,
        y=0.707,
        z=0,
        w=0.707,
    )

    DOWNWARD_POINT = Quaternion(
        x=1,
        y=0,
        z=0,
        w=0,
    )

    LEFTWARD_POINT = Quaternion(
        x=0.707,
        y=0,
        z=0,
        w=-0.707,
    )

    LEFTWARD_DIAG = Quaternion(
        x=0.5,
        y=-0.5,
        z=0,
        w=-0.707,
    )

    POSITION_1 = Quaternion(
        x=0.204954637077,
        y=0.978093304167,
        z=-0.0350698408324,
        w=0.00985856726981,
    )

    DOWNWARD_ROTATED = Quaternion(
        x=0.707,
        y=0.707,
        z=0,
        w=0,
    )

    SIDEWAYS_ORIENTED = Quaternion(x=0.5, y=-0.5, z=0.5, w=0.5)
