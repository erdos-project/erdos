import erdos
from utils import Marking, Status, Conn
from collections import OrderedDict


class Buffer():
    """
    Deque wrapper buffer for Flux fault tolerance
    """
    def __init__(self):
        self.buf = OrderedDict({})
        self.cursors = dict.fromkeys([Marking.PRIM, Marking.SEC], 0)

    def peek(self, dest: Marking.PRIM | Marking.SEC):
        idx = self.cursors[dest]
        return self.buf.items()[idx] if len(self.buf) > 0 else None

    def advance(self, dest: Marking.PRIM | Marking.SEC):
        if self.cursors[dest] < len(self.buf) - 1:
            self.cursors[dest] += 1

    def put(self, msg: erdos.Message, timestamp: erdos.Timestamp, delete: set):
        self.buf[timestamp] = (msg, {Marking.PROD})

    def ack(self, timestamp: erdos.Timestamp, dest: Marking.PRIM | Marking.SEC,
            delete: set):
        # Add destination acknowledgement to tuple's markings
        data = self.buf[timestamp]
        self.buf[timestamp][1].add(dest)
        if self.buf[timestamp][1].issubset(delete):
            del self.buf[timestamp]
        # all_timestamps = self.buf.keys()
        # while len(all_timestamps) > 0 and all_timestamps[0][0] <= timestamp:
        #     data = self.buf.popitem(last=False)
        #     ts, msg = all_timestamps[0], data[0]
        #     all_timestamps = self.buf.keys()
        return data

    def ack_all(self, dest, delete):
        return 0

    def reset(self, dest):
        return 0


def main():
    b = Buffer()
    print(type(b.peek(Marking.SEC)))


if __name__ == "__main__":
    main()
