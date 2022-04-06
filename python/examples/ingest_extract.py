"""Every second:
1) Send a number from the python script.
2) An operator squares the number.
3) The python script receives the result.
"""
import time

import erdos
from erdos.operators.map import Map


def square_msg(context, msg):
    """Squares the data from an ERDOS message."""
    print("SquareOp: received {msg}".format(msg=msg))
    return erdos.Message(context.timestamp, msg * msg)


def main():
    ingest_stream = erdos.streams.IngestStream()
    square_stream = erdos.connect_one_in_one_out(
        Map, erdos.operator.OperatorConfig(), ingest_stream, function=square_msg
    )
    extract_stream = erdos.streams.ExtractStream(square_stream)

    erdos.run_async()

    count = 0
    while True:
        timestamp = erdos.Timestamp(coordinates=[count])
        send_msg = erdos.Message(timestamp, count)
        print("IngestStream: sending {send_msg}".format(send_msg=send_msg))
        ingest_stream.send(send_msg)
        recv_msg = extract_stream.read()
        print("ExtractStream: received {recv_msg}".format(recv_msg=recv_msg))

        count += 1
        time.sleep(1)


if __name__ == "__main__":
    main()
