import logging
from typing import Union


def setup_logging(name: str, log_file: Union[str, None] = None) -> logging.Logger:
    """Create a logger with the given name and attach the given handler.

    Args:
        name: The name of the logger.
        log_file: The name of the file to log to. (console if None)

    Returns:
        A :py:class:`logging.Logger` instance that can be used to log the
        required information.
    """
    return _setup_logging(
        name,
        "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s",
        "%Y-%m-%d,%H:%M:%S",
        log_file,
    )


def setup_csv_logging(name: str, log_file: Union[str, None] = None) -> logging.Logger:
    """Create a logger that logs statistics in a CSV file, and attach the
    given handler.

    Args:
        name: The name of the logger.
        log_file: The file to log the results to. (console if None)

    Returns:
        A :py:class:`logging.Logger` instance that can be used to log the
        required information.
    """
    return _setup_logging(name, "%(message)s", None, log_file)


def setup_trace_logging(name: str, log_file: Union[str, None] = None) -> logging.Logger:
    """Create a logger that logs the runtime statistics of methods decorated
    with the :py:func:`profile_method`.

    Args:
        name: The name of the logger.
        log_file: The name of the file to log to. (console if None)

    Returns:
        A :py:class:`logging.Logger` instance that can be used to log the
        required information.
    """
    return _setup_logging(name, "%(message)s,", None, log_file)


def _setup_logging(name: str, fmt: str, date_fmt: str, log_file=None):
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
