from enum import Enum


class UpstreamControllerCommand(Enum):
    PROGRESS = -1
    FAIL = -2
    RECOVER = -3


def is_failure_stream(stream):
    return stream.labels.get('failure', '') == 'true'


def is_not_failure_stream(stream):
    return not is_failure_stream(stream)


def is_progress_stream(stream):
    return stream.labels.get('progress', '') == 'true'


def is_not_progress_stream(stream):
    return not is_progress_stream(stream)
