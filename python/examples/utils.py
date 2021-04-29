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
