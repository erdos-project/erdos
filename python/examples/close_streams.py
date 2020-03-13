import erdos


class NoopOp(erdos.Operator):
    def __init__(self, read_stream, write_stream):
        pass

    @staticmethod
    def connect(read_streams):
        return [erdos.WriteStream()]

    def destroy(self):
        print("Destroying NoopOp")


def main():
    ingest_stream = erdos.IngestStream()
    (s, ) = erdos.connect(NoopOp, erdos.OperatorConfig(), [ingest_stream])
    extract_stream = erdos.ExtractStream(s)

    handle = erdos.run_async()

    timestamp = erdos.Timestamp(is_top=True)
    send_msg = erdos.WatermarkMessage(timestamp)
    print("IngestStream: sending {send_msg}".format(send_msg=send_msg))
    ingest_stream.send(send_msg)
    assert ingest_stream.is_closed()

    recv_msg = extract_stream.read()
    print("ExtractStream: received {recv_msg}".format(recv_msg=recv_msg))
    assert recv_msg.is_top
    assert extract_stream.is_closed()

    handle.shutdown()


if __name__ == "__main__":
    main()
