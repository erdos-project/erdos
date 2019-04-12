from enum import Enum


class CheckpointControllerCommand(Enum):
    ROLLBACK = -1


def is_control_stream(stream):
    return stream.labels.get('control_stream', '') == 'true'


def is_not_control_stream(stream):
    return not is_control_stream(stream)