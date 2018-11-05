from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import Queue

import ray


@ray.remote
class BufferActor(object):
    def __init__(self, buffer_size=None):
        self._buffer_size = buffer_size
        self._stream_open = True
        self._buffer = Queue.Queue()
        self._queue_size = 0

    def close(self):
        self._stream_open = False

    def has_next(self):
        return self._stream_open

    def next(self):
        if self._queue_size == 0:
            return (False, None)
        self._queue_size -= 1
        return (True, self._buffer.get())

    def put(self, msg):
        if self._buffer_size is None or self._queue_size < self._buffer_size:
            self._queue_size += 1
            self._buffer.put(msg)
            return True
        else:
            return False
