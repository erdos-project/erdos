from enum import Enum


class FluxOperatorState(Enum):
    ACTIVE = 1
    DEAD = 2
    STAND_BY = 3
    PAUSE = 4


class ControlMsgType(Enum):
    FAILED_REPLICA = 1
    FAILED_PRIMARY = 2


def is_ack_stream(stream):
    return stream.labels.get('ack_stream', '') == 'true'


def is_not_ack_stream(stream):
    return not is_ack_stream(stream)


def is_control_stream(stream):
    return stream.labels.get('control_stream', '') == 'true'


def is_not_control_stream(stream):
    return not is_control_stream(stream)
