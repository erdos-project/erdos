import erdos
from enum import Enum


class Marking(Enum):
    PROD = 1
    PRIM = 2
    SEC = 3


class Status(Enum):
    ACTIVE = 1
    DEAD = 2
    STDBY = 3
    PAUSE = 4


class Conn(Enum):
    SEND = 1
    RECV = 2
    ACK = 3
    PAUSE = 4


def send_data(data_tuples,
              write_downstream=None,
              write_upstream=None,
              sender="Operator"):
    """
    Clears buffer of all messages <= timestamp and sends each message.
    If a write_stream is passed in, every message removed from the
    buffer will also be sent.
    """
    for msg, marking in data_tuples:
        if write_downstream is not None:
            # Send data to next operators along with watermark
            print("{name}: sending {msg}".format(name=sender, msg=msg))
            write_downstream.send(msg)
            watermark = erdos.WatermarkMessage(msg.timestamp)
            print("{name}: sending watermark {watermark} downstream".format(
                name=sender, watermark=watermark))
            write_downstream.send(watermark)
        if write_upstream is not None:
            # Send watermark acks back to previous operator
            watermark = erdos.WatermarkMessage(msg.timestamp)
            print("{name}: sending watermark {watermark} upstream".format(
                name=sender, watermark=watermark))
            write_upstream.send(watermark)
