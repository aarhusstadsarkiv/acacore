from logging import FileHandler
from logging import Formatter
from logging import getLogger
from logging import INFO
from logging import Logger
from logging import StreamHandler
from pathlib import Path
from typing import IO
from typing import overload


@overload
def setup_logger(log_name: str, *, files: list[Path], streams: list[IO] | None = None) -> Logger: ...


@overload
def setup_logger(log_name: str, *, files: list[Path] | None = None, streams: list[IO]) -> Logger: ...


def setup_logger(log_name: str, *, files: list[Path] | None = None, streams: list[IO] | None = None) -> Logger:
    """
    Set up a logger that prints to files and/or streams.

    :param log_name: The name of the logger.
    :param files: A list of Path objects representing the log files, defaults to None.
    :param streams: A list of IO objects representing the log streams, defaults to None.
    :raises AssertionError: If neither files nor streams are given.
    :return: The configured Logger object.
    """
    assert files or streams, "At least one file or stream must be set."

    files = files or []
    streams = streams or []

    logger: Logger = getLogger(log_name)
    logger_format: Formatter = Formatter(
        fmt="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.setLevel(INFO)

    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        handler: FileHandler = FileHandler(file, "a", encoding="utf-8")
        handler.setFormatter(logger_format)
        logger.addHandler(handler)

    for stream in streams:
        handler: StreamHandler = StreamHandler(stream)
        handler.setFormatter(logger_format)
        logger.addHandler(handler)

    return logger
