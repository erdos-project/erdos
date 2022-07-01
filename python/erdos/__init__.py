import logging
import sys
from functools import wraps

import erdos.context
import erdos.operator
import erdos.utils
from erdos.graph import Graph
from erdos.message import Message, WatermarkMessage
from erdos.profile import Profile
from erdos.streams import (
    EgressStream,
    IngressStream,
    LoopStream,
    OperatorStream,
    ReadStream,
    Stream,
    WriteStream,
)
from erdos.timestamp import Timestamp

# Set the top-level logger for ERDOS logging.
# Users can change the logging level to the required level by calling setLevel
# erdos.logger.setLevel(logging.DEBUG)
FORMAT = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d,%H:%M:%S"
formatter = logging.Formatter(FORMAT, datefmt=DATE_FORMAT)
default_handler = logging.StreamHandler(sys.stderr)
default_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(default_handler)
logger.setLevel(logging.WARNING)
logger.propagate = False


def profile(event_name, operator, event_data=None):
    return Profile(event_name, operator, event_data)


def profile_method(**decorator_kwargs):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if isinstance(args[0], erdos.operator.BaseOperator):
                # The func is an operator method.
                op_name = args[0].config.name
                cb_name = func.__name__
                if "event_name" in decorator_kwargs:
                    event_name = decorator_kwargs["event_name"]
                else:
                    # Set the event name to the operator name and the callback
                    # name if it's not passed by the user.
                    event_name = op_name + "." + cb_name
                timestamp = None
                if len(args) > 1:
                    if isinstance(args[1], Timestamp):
                        # The func is a watermark callback.
                        timestamp = args[1]
                    elif isinstance(args[1], Message):
                        # The func is a callback.
                        timestamp = args[1].timestamp
            else:
                raise TypeError("@erdos.profile can only be used on operator methods")

            with erdos.profile(
                event_name, args[0], event_data={"timestamp": str(timestamp)}
            ):
                return func(*args, **kwargs)

        return wrapper

    return decorator


__all__ = [
    "Graph",
    "Stream",
    "ReadStream",
    "WriteStream",
    "LoopStream",
    "IngressStream",
    "EgressStream",
    "Profile",
    "Message",
    "WatermarkMessage",
    "Timestamp",
    "add_ingress",
    "add_loop_stream",
    "connect_source",
    "connect_sink",
    "connect_one_in_one_out",
    "connect_two_in_one_out",
    "connect_one_in_two_out",
    "run",
    "run_async",
    "profile_method",
    "NodeHandle",
]
