from datetime import datetime
from logging import ERROR
from logging import INFO
from logging import Logger
from pathlib import Path
from re import compile as re_compile
from re import Pattern
from sqlite3 import DatabaseError
from sys import stdout
from traceback import format_tb
from typing import Callable
from typing import overload

from click import BadParameter
from click import Command
from click import Context
from click import Parameter

from acacore.models.history import HistoryEntry
from acacore.utils.helpers import ExceptionManager
from acacore.utils.log import setup_logger


def ctx_params(ctx: Context) -> dict[str, Parameter]:
    """
    Get parameters from a click context as a dict.

    :param ctx: The ``Context`` object of the command from which to extract parameters.
    :return: A dict of all the parameters of the context's command.
    """
    return {p.name: p for p in ctx.command.params}


def param_callback_regex(
    pattern: str,
    flags: int = 0,
) -> Callable[[Context, Parameter, str | tuple[str, ...] | None], str | tuple[str, ...] | None]:
    """
    Create a ``click.Parameter`` callback that matches the argument against a given regex pattern.

    If the value is None, the value is returned as is. If the value is a tuple (e.g., of the parameter is variadic),
    then each item of the tuple is matched. If the value is not None, str, or a tuple, then the value is returned as is.

    :param pattern: The pattern to match against.
    :param flags: The flags to use for the match.
    :return: A ``click.Parameter`` callback function with the signature ``(Context, Parameter, T) -> T``.
    """
    compiled_pattern: Pattern = re_compile(pattern, flags)

    def callback(ctx: Context, param: Parameter, value: str | tuple[str, ...] | None) -> str | tuple[str, ...] | None:
        if value is None:
            return value
        elif isinstance(value, str) and not compiled_pattern.match(value):  # noqa: SIM114
            raise BadParameter(f"does not match {pattern!r}", ctx, param)
        elif isinstance(value, tuple) and any(not compiled_pattern.match(v) for v in value):
            raise BadParameter(f"does not match {pattern!r}", ctx, param)
        return value

    return callback


def copy_params(command: Command) -> Callable[[Command], Command]:
    """
    Copy parameters from one ``Command`` to another.

    :param command: The command from which to copy the parameters.
    """

    def decorator(command2: Command) -> Command:
        command2.params.extend(command.params.copy())
        return command2

    return decorator


def check_database_version(ctx: Context, param: Parameter, path: Path):
    """
    Check if the database at ``path`` is the latest version or not.

    :param ctx: The context of the parameter.
    :param param: The parameter from which the path value originates.
    :param path: The path to the database.
    :raises BadParameter: If the database version is not the latest.
    """
    if not path.is_file():
        return

    from acacore.database import FileDB
    from acacore.database.upgrade import is_latest

    with FileDB(path, check_version=False) as db:
        try:
            is_latest(db, raise_on_difference=True)
        except DatabaseError as err:
            raise BadParameter(err.args[0], ctx, param)


def start_program(
    ctx: Context,
    database: "FileDB",  # noqa: F821
    version: str,
    time: datetime | None = None,
    log_file: bool = True,
    log_stdout: bool = True,
    dry_run: bool = False,
) -> tuple[Logger | None, Logger | None, HistoryEntry]:
    """
    Create loggers and ``HistoryEntry`` for the start of a click program.

    If ``log_file`` is ``False``, the file logger return value is ``None``. If ``log_stdout`` is ``False``, the
    standard output logger return value is ``None``.

    If ``dry_run`` is ``True``, the start event is not added to the database.

    :param ctx: The context of the command that should be logged.
    :param database: The database instance.
    :param version: The version of the command/program.
    :param time: Optionally, the time to use for the ``HistoryEntry`` event. Defaults to now.
    :param log_file: Whether a file log should be opened and returned. Defaults to ``False``.
    :param log_stdout: Whether a standard output log should be opened and returned. Defaults to ``False``.
    :param dry_run: Whether the command is run in dry-run mode.
    :return: A tuple containing the file logger (if set with ``log_file`` otherwise ``None``), the standard output
        logger (if set with ``log_stdout`` otherwise ``None``), and the ``HistoryEntry`` start event.
    """
    prog: str = ctx.find_root().command.name
    log_file: Logger | None = (
        setup_logger(f"{prog}_file", files=[database.path.parent / f"{prog}.log"]) if log_file else None
    )
    log_stdout: Logger | None = setup_logger(f"{prog}_stdout", streams=[stdout]) if log_stdout else None
    program_start: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "start",
        data={"version": version},
        add_params_to_data=True,
        time=time,
    )

    if not dry_run:
        database.history.insert(program_start)

    if log_file:
        program_start.log(INFO, log_file)
    if log_stdout:
        program_start.log(INFO, log_stdout, show_args=False)

    return log_file, log_stdout, program_start


def end_program(
    ctx: Context,
    database: "FileDB",  # noqa: F821
    exception: ExceptionManager,
    dry_run: bool = False,
    *loggers: Logger | None,
):
    program_end: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "end",
        data=repr(exception.exception) if exception.exception else None,
        reason="".join(format_tb(exception.traceback)) if exception.traceback else None,
    )

    for logger in loggers:
        if logger:
            program_end.log(ERROR if exception.exception else INFO, logger)

    if not dry_run:
        database.history.insert(program_end)
        database.commit()
