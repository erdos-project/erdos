import numpy as np
import random
import time


def do_work(logger, min_runtime_us, max_runtime_us):
    runtime_us = np.random.randint(min_runtime_us, max_runtime_us + 1)
    logger.info("Sleeping for %s us", runtime_us)
    runtime = runtime_us / 1000000.0
    cur_time = time.time()
    while time.time() < cur_time + runtime:
        pass


def generate_synthetic_bounding_boxes(min_det_objs, max_det_objs):
    det_objs = np.random.randint(min_det_objs, max_det_objs + 1)
    bboxes = []
    for i in range(0, det_objs, 1):
        cat = 'person'
        score = random.uniform(0, 1)
        x = random.randint(0, 1024)
        y = random.randint(0, 1024)
        w = random.randint(0, 1024)
        h = random.randint(0, 1024)
        bboxes.append((cat, score, (x, y, w, h)))
    return bboxes
