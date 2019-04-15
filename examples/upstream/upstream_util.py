from enum import Enum


class UpstreamControllerCommand(Enum):
    PROGRESS = -1
    FAIL = -2
    RECOVER = -3


def is_control_stream(stream):
    return stream.labels.get('control_stream', '') == 'true'


def is_not_control_stream(stream):
    return not is_control_stream(stream)