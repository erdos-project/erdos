"""Every second:
1) Send a number from the python script.
2) An operator squares the number.
3) The python script receives the result.
"""
import time

import erdos

from erdos.graph import Graph


def square_msg(context, msg):
    """Squares the data from an ERDOS message."""
    print("SquareOp: received {msg}".format(msg=msg))
    return erdos.Message(context.timestamp, msg * msg)


def main():
    graph = Graph()

    ingress_stream = graph.add_ingress("IngressStream")
    square_stream = ingress_stream.map(lambda x: x * x)

    egress_stream = square_stream.to_egress()

    graph.run_async()

    count = 0
    while True:
        timestamp = erdos.Timestamp(coordinates=[count])
        send_msg = erdos.Message(timestamp, count)
        print("IngestStream: sending {send_msg}".format(send_msg=send_msg))
        ingress_stream.send(send_msg)
        recv_msg = egress_stream.read()
        print("ExtractStream: received {recv_msg}".format(recv_msg=recv_msg))

        count += 1
        time.sleep(1)


if __name__ == "__main__":
    main()
