from datetime import datetime
from logging import CRITICAL
from logging import DEBUG
from logging import ERROR
from logging import FileHandler
from logging import Formatter
from logging import getLevelName
from logging import getLogger
from logging import INFO
from logging import Logger
from logging import WARNING
from pathlib import Path
from random import random
from uuid import UUID
from uuid import uuid4

import pytest
from click import argument
from click import command
from click import Context
from click import option
from click import pass_context

from acacore.__version__ import __version__
from acacore.models.event import Event


def _get_log_output(log_file: Path) -> list[str]:
    return [line for line in map(str.strip, log_file.read_text().splitlines()) if line]


@pytest.fixture
def log_file(temp_folder: Path):
    return temp_folder / f"{Path(__file__).name}.log"


@pytest.fixture
def logger(log_file: Path) -> Logger:
    logger: Logger = getLogger(log_file.name)
    logger_format: Formatter = Formatter(fmt="%(levelname)s: %(message)s")
    handler: FileHandler = FileHandler(log_file)
    handler.setFormatter(logger_format)
    logger.setLevel(DEBUG)
    logger.addHandler(handler)
    return logger


def test_event_log(log_file: Path, logger: Logger):
    uuid: UUID = uuid4()
    time: datetime = datetime.now()
    operation: str = f"{Path(__file__).name}:test_event_log"
    data: dict[str, float] = {"random": random()}
    reason: str = "test"
    extra: tuple[str, float] = ("extra", random())

    event: Event = Event(file_uuid=uuid, file_type="original", time=time, operation=operation, data=data, reason=reason)
    expected: str

    for level in (CRITICAL, ERROR, WARNING, INFO, DEBUG):
        expected = f"{getLevelName(level)}: {operation} uuid=original:{uuid} data={data} reason={reason}"
        event.log(level, logger)
        assert _get_log_output(log_file)[-1] == expected

    expected = f"{getLevelName(INFO)}: {operation} uuid=original:{uuid} {extra[0]}={extra[1]}"
    event.data = None
    event.log(INFO, logger, show_args=["uuid"], show_null=False, **dict([extra]))
    assert _get_log_output(log_file)[-1] == expected


def test_event_from_command(log_file: Path, logger: Logger):
    uuid: UUID = uuid4()
    time: datetime = datetime.now()
    operation: str = "test_event_from_command"
    data: dict[str, float] = {"random": random()}
    reason: str = "test"

    @command("test-app")
    @argument("arg", nargs=-1)
    @option("--value-option", type=int)
    @pass_context
    def _app(ctx: Context, *_, **__) -> tuple[Event, Context]:
        return Event.from_command(ctx, operation, (uuid, "original"), data, reason, time, add_params_to_data=True), ctx

    args: tuple[str, ...] = ("hello world",)
    value_option: int = 42
    event: Event
    context: Context
    event, context = _app.main([*args, "--value-option", str(value_option)], standalone_mode=False)
    assert event.file_uuid == uuid
    assert event.file_type == "original"
    assert event.time == time
    assert event.operation == f"{_app.name}:{operation}"
    assert event.data == data | {"acacore": __version__, "params": {"arg": args, "value_option": value_option}}
    assert event.reason == reason
    event.log(INFO, logger)
