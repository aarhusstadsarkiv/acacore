from collections.abc import Callable
from datetime import datetime
from logging import ERROR
from logging import INFO
from logging import Logger
from pathlib import Path
from re import compile as re_compile
from re import Pattern
from sqlite3 import DatabaseError
from traceback import format_tb

from click import BadParameter
from click import ClickException
from click import Command
from click import Context
from click import MissingParameter
from click import Parameter
from structlog.stdlib import BoundLogger
from structlog.stdlib import get_logger

from acacore.database import query
from acacore.utils.helpers import ExceptionManager


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


def param_callback_query(
    required: bool,
    default: str,
    allowed_fields: list[str] | None = None,
) -> Callable[[Context, Parameter, str], query.QueryTokens]:
    def _callback(ctx: Context, param: Parameter, value: str | None) -> tuple[str | None, list[str]]:
        if not (value := value or "").strip() and required:
            raise MissingParameter(None, ctx, param)
        if not value:
            return None, []

        try:
            tokens = query.tokenizer(value, default, allowed_fields or [])
            if not tokens and required:
                raise BadParameter("no values in query.", ctx, param)
            return query.tokens_to_where(tokens)
        except ClickException:
            raise
        except FileNotFoundError as err:
            raise BadParameter(f"{err.filename} file not found", ctx, param)
        except ValueError as err:
            raise BadParameter(err.args[0], ctx, param)
        except Exception as err:
            raise BadParameter(repr(err), ctx, param)

    return _callback


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

    from acacore.database import FilesDB
    from acacore.database.upgrade import is_latest

    with FilesDB(path) as db:
        try:
            is_latest(db.connection, raise_on_difference=True)
        except DatabaseError as err:
            raise BadParameter(err.args[0], ctx, param)


def context_commands(ctx: Context) -> list[str]:
    current: Context = ctx
    command_parts: list[str] = [current.command.name]

    while current.parent is not None:
        current = current.parent
        command_parts.insert(0, current.command.name)

    return command_parts


def start_program(
    ctx: Context,
    database: "FilesDB",  # noqa: F821
    version: str,
    time: datetime | None = None,
    dry_run: bool = False,
) -> tuple[BoundLogger, "Event"]:  # noqa: F821
    """
    Setup logger and ``Event`` for the start of a click program.

    :param ctx: The context of the command that should be logged.
    :param database: The database instance.
    :param version: The version of the command/program.
    :param time: Optionally, the time to use for the ``Event`` object. Defaults to now.
    :param dry_run: Whether the command is run in dry-run mode.
    :return: A tuple containing the logger and the ``Event`` object for the start of the program.
    """
    from acacore.models.event import Event

    prog: str = ctx.find_root().command.name
    logger: BoundLogger = get_logger(f"{prog}_file")
    program_start: Event = Event.from_command(
        ctx,
        "start",
        data={"version": version},
        add_params_to_data=True,
        time=time,
    )

    if not dry_run:
        database.log.insert(program_start)

    program_start.log(INFO, logger, show_args=False)

    return logger, program_start


def end_program(
    ctx: Context,
    database: "FilesDB",  # noqa: F821
    exception: ExceptionManager,
    dry_run: bool = False,
    *loggers: Logger | BoundLogger | None,
):
    """
    Create ``Event`` for the end of a click program.

    If ``dry_run`` is ``True``, the end event is not added to the database, and the database changes are not committed.

    :param ctx: The context of the command that should be logged.
    :param database: The database instance.
    :param exception: An ``ExceptionManager`` object that wrapped the command execution.
    :param dry_run: Whether the command was run in dry-run mode.
    :param loggers: A list of loggers the end event should be logged with.
    """
    from acacore.models.event import Event

    program_end: Event = Event.from_command(
        ctx,
        "end",
        data=repr(exception.exception) if exception.exception else None,
        reason="".join(format_tb(exception.traceback)) if exception.traceback else None,
    )

    for logger in loggers:
        if logger:
            program_end.log(ERROR if exception.exception else INFO, logger)

    if not dry_run:
        database.log.insert(program_end)
        database.commit()
