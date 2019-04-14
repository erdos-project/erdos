import logging
import time
from functools import wraps
from threading import Thread, Condition

_freq_called = set([])


def deadline(*expected_args):
    """
    Deadline decorator to be used for restraining computation latency.
    Takes in computation's duration constrain in ms and
    the name of the function to call when the deadline is missed
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            condition = Condition()

            def check_deadline(end_time):
                with condition:
                    condition.wait(timeout=end_time - time.time())
                if time.time() >= end_time:
                    getattr(args[0], expected_args[1])()

            deadline_time = time.time() + expected_args[0] / 1000.0
            check_thread = Thread(target=check_deadline, args=(deadline_time,))
            check_thread.start()
            func(*args, **kwargs)  # Execute callback function
            with condition:
                condition.notify()
            check_thread.join()
        return wrapper

    return decorator


def frequency(*expected_args):
    """ Frequency decorator to be used for periodic tasks (i.e., methods)."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            framework = args[0].framework
            if framework == "ros":
                import rospy
                rate = rospy.Rate(expected_args[0])
                while not rospy.is_shutdown():
                    func(*args, **kwargs)
                    rate.sleep()
            elif framework == "ray":
                # XXX(ionel): Hack to avoid recursive calls. frequency()
                # method is called upon each func invocation, thus without the
                # _freq_called check the func would be called infinitely. The
                # fix is a hack because set_frequency is first called from main
                # thread, and once from the actor worker thread because
                # _freq_called is local.
                func_id = (args[0].name, func.__name__)
                if func_id not in _freq_called:
                    _freq_called.add(func_id)
                    # Remove reference to self because is added again when
                    # the callback is invoked.
                    method_args = args[1:]
                    args[0].freq_actor.set_frequency.remote(
                        expected_args[0], func.__name__, *method_args)
                else:
                    func(*args, **kwargs)

        return wrapper

    return decorator


def setup_logging(name, log_file=None):
    if log_file is None:
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(log_file)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S')
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def setup_csv_logging(name, log_file=None):
    if log_file is None:
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(log_file)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt='%(message)s',
        datefmt=None)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def time_epoch_ms():
    return int(time.time() * 1000)


def log_graph_to_dot_file(filename, nodes, edges):
    dot_file = open(filename, 'w')
    # dot header
    dot_file.write("digraph G {")
    dot_file.write("\tgraph [rankdir=\"LR\"]")
    # nodes
    dot_file.write("\t{ node [shape=box]")
    dot_file.write("\t")
    for n in nodes:
        # TODO(ionel): We replace / to _ because dot doesn't support / in
        # node names. Implement a better solution.
        node = n.replace("/", "_")
        dot_file.write("{} [ label = \"{}\" ];".format(node, node))
    dot_file.write("\t}")
    dot_file.write("\t{ edge [color=\"#cccccc\"]")
    dot_file.write("\t")
    for (src_n, dst_n, name) in edges:
        src_node = src_n.replace("/", "_")
        dst_node = dst_n.replace("/", "_")
        dot_file.write("{} -> {} [ label = \"{}\" ];".format(
            src_node, dst_node, name))
    dot_file.write("\t}")
    # dot footer
    dot_file.write("}")
    dot_file.close()
