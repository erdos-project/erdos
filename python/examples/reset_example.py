"""Creates and runs a dataflow.
Then shuts down and resets the dataflow, and runs a different dataflow.
"""
import time

import erdos
from erdos.operators import window, map


def double(msg):
    msg.data = 2 * msg.data
    return msg


def square(msg):
    msg.data = msg.data * msg.data
    return msg


def main():
    # Create the first dataflow.
    ingest_stream = erdos.IngestStream()
    (map_stream, ) = erdos.connect(map.Map,
                                   erdos.OperatorConfig(name="Double"),
                                   [ingest_stream],
                                   function=double)
    extract_stream = erdos.ExtractStream(map_stream)
    node_handle = erdos.run_async()

    for t in range(5):
        send_msg = erdos.Message(erdos.Timestamp(coordinates=[t]), t)
        ingest_stream.send(send_msg)
        print("IngestStream: sent {send_msg}".format(send_msg=send_msg))
        recv_msg = extract_stream.read()
        print("ExtractStream: received {recv_msg}".format(recv_msg=recv_msg))

        time.sleep(1)

    node_handle.shutdown()
    erdos.reset()

    # Create a new dataflow.
    ingest_stream = erdos.IngestStream()
    (map_stream, ) = erdos.connect(map.Map,
                                   erdos.OperatorConfig(name="Square"),
                                   [ingest_stream],
                                   function=square)
    extract_stream = erdos.ExtractStream(map_stream)
    node_handle = erdos.run_async()

    for t in range(5):
        send_msg = erdos.Message(erdos.Timestamp(coordinates=[t]), t)
        ingest_stream.send(send_msg)
        print("IngestStream: sent {send_msg}".format(send_msg=send_msg))
        recv_msg = extract_stream.read()
        print("ExtractStream: received {recv_msg}".format(recv_msg=recv_msg))

        time.sleep(1)

    node_handle.shutdown()


if __name__ == "__main__":
    main()
