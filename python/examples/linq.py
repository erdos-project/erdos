import time
from typing import Any

import erdos
from erdos.context import SinkContext
from erdos.operator import OperatorConfig, Sink, Source
from erdos.streams import WriteStream


class SendOp(Source):
    """A :py:class:`SendOp` is a :py:class:`Source` operator that generates a sequence
    of inputs for the dataflow graph."""

    def __init__(self):
        print("Initializing SendOp")

    def run(self, write_stream: WriteStream):
        count = 0
        while True:
            msg = erdos.Message(erdos.Timestamp(coordinates=[count]), count)
            print(f"SendOp sending {msg}")
            write_stream.send(msg)
            count += 1
            time.sleep(1)


class SinkOp(Sink):
    """A :py:class:`SinkOp` is a :py:class:`Sink` operator that prints the received
    output to the standard output."""

    def on_data(self, context: SinkContext, data: Any):
        print(
            f"SinkOp ({context.config.name}): Received data: {data} for "
            f"timestamp: {context.timestamp}"
        )


def main():
    source_stream = erdos.connect_source(SendOp, OperatorConfig())
    map_stream = source_stream.map(lambda x: x**2)
    evens_stream, odds_stream = map_stream.split(lambda x: x % 2 == 0)
    flat_map_stream = map_stream.flat_map(lambda x: (f"Number {x}", float(x / 2)))
    str_stream, float_stream = flat_map_stream.split_by_type(str, float)
    merged_stream = evens_stream.concat(odds_stream, str_stream, float_stream)
    erdos.connect_sink(SinkOp, OperatorConfig(name="MergedOutput"), merged_stream)
    erdos.run()


if __name__ == "__main__":
    main()
