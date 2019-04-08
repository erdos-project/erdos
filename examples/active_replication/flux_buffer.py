
class Buffer:
    def __init__(self, n_dest):
        self.n_dest = n_dest
        self.queue = list()  # TODO(yika): add upper bound on buffer size
        self.cursors = [-1 for _ in range(n_dest)]  # first undelivered tuple

    def peek(self, dest):
        # return first undelivered tuple
        if -1 < self.cursors[dest] < len(self.queue):
            return self.queue[self.cursors[dest]]

    def advance(self, dest):
        # Move cursor to next undelivered tuple
        self.cursors[dest] += 1

    def put(self, data, sn):
        # Put input stream
        if len(self.queue) > 0:
            assert sn > self.queue[-1][0]
        ack_status = tuple([False for _ in range(self.n_dest)])
        self.queue.append((sn, data, ack_status))

    def ack(self, sn, dest):
        for i in range(len(self.queue)):
            item = self.queue[i]
            if item[0] == sn:
                temp = list(item[2])
                temp[dest] = True
                if all(temp):
                    self.queue.pop(i)
                else:
                    self.queue[i] = (item[0], item[1], tuple(temp))
                return True
        return False

    def ack_all(self, dest):
        # called when dest faileds
        self.n_dest -= 1
        self.cursors.pop(dest)
        for i in range(len(self.queue)):
            new_status = tuple(list(self.queue[i][2]).pop(dest))
            if all(new_status):
                self.queue.pop(i)
            else:
                self.queue[i] = (self.queue[i][0], self.queue[i][1], new_status)

    def reset(self):
        self.n_dest += 1
        self.cursors.append(0)
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
