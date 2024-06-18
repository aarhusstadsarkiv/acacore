from datetime import datetime
from logging import Logger

from click import Context
from pydantic import Field
from pydantic import UUID4

from acacore.__version__ import __version__
from acacore.database.column import DBField

from .base import ACABase


class HistoryEntry(ACABase):
    uuid: UUID4 | None = DBField(default=None, index=["idx_uuid_history"])
    time: datetime = Field(default_factory=datetime.now)
    operation: str
    data: object | None = None
    reason: str | None = None

    @classmethod
    def command_history(
        cls,
        ctx: Context | str,
        operation: str,
        uuid: UUID4 | None = None,
        data: object | None = None,
        reason: str | None = None,
        time: datetime | None = None,
        add_params_to_data: bool = False,
    ) -> "HistoryEntry":
        """
        Create a HistoryEntry for a command.

        :param ctx: The context object representing the current command execution.
        :param operation: The name of the operation for which the command history entry is being created.
        :param uuid: Optional. The UUID associated with the command history entry, defaults to None.
        :param data: Optional. Additional data or parameters associated with the command history entry.
            It Context object, otherwise None, defaults to None.
        :param reason: Optional. The reason for the command execution, defaults to None.
        :param time: Optional. The timestamp of the command execution, defaults to None.
        :param add_params_to_data: If true, add context parameters to data, defaults to False.
        :return: A `HistoryEntry` instance representing the command history entry.
        """
        command: str

        if isinstance(ctx, Context):
            current: Context = ctx
            command_parts: list[str] = [current.command.name]
            while current.parent is not None:
                current = current.parent
                command_parts.insert(0, current.command.name)
            command = ".".join(command_parts)
        else:
            command = ctx

        operation = f"{command.strip(':.')}:{operation.strip(':')}"

        if add_params_to_data and not isinstance(ctx, Context):
            raise TypeError(f"add_params_to_data is not compatible with ctx of type {type(ctx)}")

        if add_params_to_data and data is None:
            data = {"acacore": __version__, "params": ctx.params}
        elif add_params_to_data and isinstance(data, dict):
            data |= {"acacore": __version__, "params": ctx.params}
        elif add_params_to_data and isinstance(data, list):
            data.append({"acacore": __version__, "params": ctx.params})
        elif add_params_to_data:
            raise TypeError(f"Data type {type(data)} is not compatible with add_params_to_data")

        return cls(
            uuid=uuid,
            time=time or datetime.now(),
            operation=operation,
            data=data,
            reason=reason,
        )

    def log(self, level: int, *logger: Logger, show_null: bool = True):
        if show_null:
            msg: str = f"{self.operation} {self.uuid=} {self.data=} {self.reason=}"
        else:
            msg: str = (
                f"{self.operation}"
                + (f" {self.uuid=}" if self.uuid is not None else "")
                + (f" {self.data=}" if self.data is not None else "")
                + (f" {self.reason=}" if self.reason is not None else "")
            )

        for logger in logger:
            logger.log(level, msg)
