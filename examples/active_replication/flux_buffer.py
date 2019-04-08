
class Buffer:
    def __init__(self, n_dest):
        self.n_dest = n_dest
        self.queue = list()  # TODO(yika): add upper bound on buffer size

    def put(self, data, sn, dest):
        # Put input stream
        if len(self.queue) > 0:
            assert sn >= self.queue[-1][0]
        self.queue.append((sn, data, dest))

    def ack(self, sn, dest):
        for i in range(len(self.queue)):
            if self.queue[i][0] == sn and self.queue[i][2] == dest:
                self.queue.pop(i)
                return True
        return False

    def ack_all(self, dest):
        i = 0
        while i < len(self.queue):
            if self.queue[i][2] == dest:
                self.queue.pop(i)
            else:
                i += 1

    def reset(self):
        self.n_dest += 1
        for i in range(len(self.queue)):
            new_status = self.queue[i][2] + (False,)
            self.queue[i] = (self.queue[i][0], self.queue[i][1], new_status)

    def _drop(self, i):
        # Drop if all ack status are either True or None (failed)
        for temp in self.queue[i][2]:
            if temp is False:
                return
        self.queue.pop(i)

    def size(self):
        return len(self.queue)

    def match_oldest(self, sn):
        return self.queue[0][0] == sn

    def pop_oldest(self):
        self.queue.pop(0)

    def send_and_clear(self, pub):
        for data in self.queue:
            pub.send(data[1])
        self.queue = list()

