from collections import OrderedDict
import erdos


class Buffer():
    """
    Deque wrapper buffer for Flux fault tolerance
    """
    def __init__(self, init_buffer=[]):
        self.buf = OrderedDict(init_buffer)

    def put(self, msg: erdos.Message, timestamp: erdos.Timestamp, delete: set):
        """ Inserts a new tuple into the buffer with
        (message, markings needed to remove message from buffer) """
        assert timestamp not in self.buf, "Duplicate key: msg for timestamp"

        self.buf[timestamp] = (msg, delete)

    def watermark(self, timestamp: erdos.Timestamp, clear=False):
        """ When watermark received from upstream operator, ret messages <= timestamp
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
        """ Need to implement still """
        return 0

    def reset(self, dest):
        """ Need to implement still """
        return 0

    def __repr__(self):
        return str(self.buf)
