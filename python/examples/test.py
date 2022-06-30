import time

import erdos

from erdos.graph import Graph
from erdos.context import SinkContext
from erdos.operator import Sink, Source
from erdos.streams import ReadStream, WriteStream


class SendOp(Source):
    def __init__(self):
        print("initializing source op")

    def run(self, write_stream: WriteStream):
        count = 0
        for _ in range(10):
            msg = erdos.Message(erdos.Timestamp(coordinates=[count]), count)
            print("SendOp: sending {msg}".format(msg=msg))
            write_stream.send(msg)

            count += 1
            time.sleep(1)


graph = Graph()
ingress_stream1 = graph.add_ingress("IngressStream1")
# ingress_stream2 = graph.add_ingress("IngressStream2")
loop_stream = graph.add_loop_stream()
print(loop_stream)

source_stream = graph.connect_source(SendOp, erdos.operator.OperatorConfig())

loop_stream.connect_loop(source_stream)

egress_stream = source_stream.to_egress()
print(egress_stream)

graph.run_async()

for _ in range(10):
    msg = erdos.Message(erdos.Timestamp(coordinates=[1]), 1)
    ingress_stream1.send(msg)

time.sleep(5)