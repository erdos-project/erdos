import logging


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
