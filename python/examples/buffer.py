import erdos
from utils import Marking, Status, Conn
from collections import OrderedDict


class Buffer():
    """
    Deque wrapper buffer for Flux fault tolerance
    """
    def __init__(self, init_buffer=[]):
        self.buf = OrderedDict(init_buffer)
        # self.cursors = dict.fromkeys([Marking.PRIM, Marking.SEC], 0)

    # def __update_cursors(self, timestamp):
    #     idx = self.cursors[Marking.PRIM]
    #     timestamps = list(self.buf.keys())
    #     if timestamps[idx] > timestamp and idx > 0:
    #         self.cursors[Marking.PRIM] -= 1
    #     idx = self.cursors[Marking.SEC]
    #     if timestamps[idx] > timestamp and idx > 0:
    #         self.cursors[Marking.SEC] -= 1

    # def peek(self, dest: Marking):
    #     """ Returns the tuple at the current cursor position at this dest """
    #     idx = self.cursors[dest]
    #     return list(self.buf.values())[idx] if len(self.buf) > 0 else None

    # def advance(self, dest: Marking):
    #     """ Advances cursor index forward for dest by 1 """
    #     if self.cursors[dest] < len(self.buf) - 1:
    #         self.cursors[dest] += 1

    def put(self, msg: erdos.Message, timestamp: erdos.Timestamp, delete: set):
        """ Inserts a new tuple into the buffer with
        (message, markings needed to remove message from buffer) """
        assert timestamp not in self.buf, "Duplicate key: msg for timestamp exists"

        self.buf[timestamp] = (msg, delete)

    # def ack(self, timestamp: erdos.Timestamp, dest: Marking, delete: set):
    #     """ When you receive an ack from a downstream operator """
    #     # Add destination acknowledgement to tuple's markings
    #     data_tuples = []
    #     buf_items = self.buf.items()
    #     for ts, data in buf_items:
    #         if ts > timestamp:
    #             break

    #         data_tuples.append(data)
    #         self.buf[ts][1].add(dest)
    #         if delete.issubset(self.buf[timestamp][1]):
    #             del self.buf[ts]

    #     return data_tuples

    # def add_dest_ack(self, timestamp: erdos.Timestamp, dest=set({})):
    #     assert timestamp in self.buf, "timestamp doesn't exist in buffer"

    #     self.buf[timestamp][1].update(dest)

    def watermark(self, timestamp: erdos.Timestamp, clear=False):
        """ When you receive a watermark from an upstream operator, returns all messages <= timestamp
        and clears the buffer if clear is set to true
        """
        data_tuples = []
        buf_items = self.buf.items()
        for ts, data in buf_items:
            if ts > timestamp:
                break

            data_tuples.append(data)
            if clear:
                del self.buf[ts]

        return data_tuples

    def ack_all(self, dest, delete):
        return 0

    def reset(self, dest):
        return 0

    def __repr__(self):
        return str(self.buf)


def main():
    b = Buffer()
    print(type(b.peek(Marking.SEC)))


if __name__ == "__main__":
    main()
