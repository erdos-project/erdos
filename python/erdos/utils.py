import logging


def setup_logging(name, log_file=None):
    return _setup_logging(
        name, "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s",
        "%Y-%m-%d,%H:%M:%S", log_file)


def setup_csv_logging(name, log_file=None):
    return _setup_logging(name, "%(message)s", None, log_file)


def setup_trace_logging(name, log_file=None):
    return _setup_logging(name, "%(message)s,", None, log_file)


def _setup_logging(name, fmt, date_fmt, log_file=None):
    if log_file is None:
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(log_file)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
