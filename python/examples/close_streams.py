"""Creates a dummy operator and gracefully shuts it down.
"""

import erdos
from erdos.graph import Graph
from erdos.operator import OneInOneOut


class NoopOp(OneInOneOut):
    def __init__(self):
        print("Initializing NoopOp")

    def destroy(self):
        print("Destroying NoopOp")


def main():
    graph = Graph()

    ingress_stream = graph.add_ingress("IngressStream")
    s = graph.connect_one_in_one_out(
        NoopOp, erdos.operator.OperatorConfig(), ingress_stream
    )
    egress_stream = s.to_egress()

    handle = graph.run_async()

    timestamp = erdos.Timestamp(is_top=True)
    send_msg = erdos.WatermarkMessage(timestamp)
    print("IngressStream: sending {send_msg}".format(send_msg=send_msg))
    ingress_stream.send(send_msg)
    assert ingress_stream.is_closed()

    recv_msg = egress_stream.read()
    print("EgressStream: received {recv_msg}".format(recv_msg=recv_msg))
    assert recv_msg.is_top
    assert egress_stream.is_closed()

    handle.shutdown()


if __name__ == "__main__":
    main()
