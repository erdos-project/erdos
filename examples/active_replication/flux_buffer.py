
class Buffer:
    def __init__(self, n_dest):
        self.n_dest = n_dest
        self.queue = list()  # TODO(yika): add upper bound on buffer size

    def put(self, data, sn, dest):
        # Put input stream
        if len(self.queue) > 0:
            # current seq number needs to be bigger or equal to the last inserted seq number
            # last inserted item can have the same seq number but different destination
            assert sn >= self.queue[-1][0]
        self.queue.append((sn, data, dest))
        # TODO(yika): alternatively we could replace dest with num_non_acknowledged

    def ack(self, sn, dest):
        # This is rarely O(N) becasue per single replica we get the ACK in sequence order,
        # only if ACKs come from different replicas, we can get a little bit of asynchrony.
        # Thus, we rarely iterate over more than the first few buffered messages.
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

    def size(self):
        return len(self.queue)

    def match_oldest(self, sn):
        return self.queue[0][0] == sn

    def pop_oldest(self):
        if self.size() == 0:
            return None
        return self.queue.pop(0)

    def reset(self):
        # TODO(yika): for recovery, currently not used
        self.n_dest += 1
        for i in range(len(self.queue)):
            new_status = self.queue[i][2] + (False,)
            self.queue[i] = (self.queue[i][0], self.queue[i][1], new_status)
