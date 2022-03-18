from typing import Any

from erdos.context import OneInOneOutContext
from erdos.operator import OneInOneOut


class Map(OneInOneOut):
    """Applies the provided function to a message and sends the resulting
    message."""

    def __init__(self, function: Any):
        self.function = function

    def on_data(self, context: OneInOneOutContext, data: Any):
        msg = self.function(context, data)
        context.write_stream.send(msg)
