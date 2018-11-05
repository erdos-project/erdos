from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import itertools
import ray
import sys
import time
from absl import app
from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_integer('num_actors', 1, 'Number of actors.')
flags.DEFINE_integer('num_requests', 10000, 'Number of requests.')


@ray.remote
class RPCActor(object):
    def __init__(self):
        pass

    def serve(self, req):
        ret = do_work.remote(req)
        return [ret]


@ray.remote
class NoOpActor(object):
    def __init__(self):
        pass

    def serve(self, req):
        return req


@ray.remote
def do_work(req):
    return req


def run_rpc_actors():
    rpc_actors = []
    for i in range(0, FLAGS.num_actors):
        rpc_actors.append(RPCActor.remote())
    rets = []
    start_time = time.time()
    rets = [rpc_actors[i % FLAGS.num_actors].serve.remote(i)
            for i in range(0, FLAGS.num_requests, 1)]
    ray.get(list(itertools.chain(*ray.get(rets))))
    duration = time.time() - start_time
    print('Duration RPC actors {}'.format(duration))


def run_no_op_actors():
    no_op_actors = []
    for i in range(0, FLAGS.num_actors):
        no_op_actors.append(NoOpActor.remote())
    start_time = time.time()
    rets = [no_op_actors[i % FLAGS.num_actors].serve.remote(i)
            for i in range(0, FLAGS.num_requests, 1)]
    ray.get(rets)
    duration = time.time() - start_time
    print('Duration no op actors {}'.format(duration))


def run_tasks():
    start_time = time.time()
    rets = [do_work.remote(i) for i in range(0, FLAGS.num_requests, 1)]
    ray.get(rets)
    duration = time.time() - start_time
    print('Duration tasks {}'.format(duration))


def main(argv):
    ray.init(redirect_output=True)
    run_rpc_actors()
    run_no_op_actors()
    run_tasks()


if __name__ == '__main__':
    app.run(main)
