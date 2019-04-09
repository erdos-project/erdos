from enum import Enum


class FluxOperatorState(Enum):
    ACTIVE = 1
    DEAD = 2
    STAND_BY = 3
    PAUSE = 4


class FluxControllerCommand(Enum):
    FAIL = -1
    RECOVER = -2


class SpecialCommand(Enum):
    REVERSE = -2


def is_ack_stream(stream):
    return stream.labels.get('ack_stream', '') == 'true'


def is_not_ack_stream(stream):
    return not is_ack_stream(stream)


def is_not_back_pressure(stream):
    return not stream.labels.get('back_pressure', '') == 'true'


def is_flux_consumer_output(stream):
    return stream.labels.get('back_pressure', '') == 'true'


def is_control_stream(stream):
    return stream.labels.get('control_stream', '') == 'true'


def is_not_control_stream(stream):
    return not is_control_stream(stream)
