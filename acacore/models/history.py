from datetime import datetime
from typing import Optional

from click import Context
from pydantic import Field
from pydantic import UUID4

from acacore.__version__ import __version__
from acacore.database.column import DBField

from .base import ACABase


class HistoryEntry(ACABase):
    uuid: Optional[UUID4] = DBField(default=None, index=["idx_uuid_history"])
    time: datetime = Field(default_factory=datetime.now)
    operation: str
    data: Optional[object] = None
    reason: Optional[str] = None

    @classmethod
    def command_history(
        cls,
        ctx: Context,
        operation: str,
        uuid: Optional[UUID4] = None,
        data: Optional[object] = None,
        reason: Optional[str] = None,
        time: Optional[datetime] = None,
        add_params_to_data: bool = False,
    ) -> "HistoryEntry":
        """
        Create a HistoryEntry for a command.

        Args:
            ctx: The context object representing the current command execution.
            operation: The name of the operation for which the command history entry is being created.
            uuid: Optional. The UUID associated with the command history entry. Default is None.
            data: Optional. Additional data or parameters associated with the command history entry.
                It defaults to the params property of the context, if it is a click.Context object, otherwise None.
            reason: Optional. The reason for the command execution. Default is None.
            time: Optional. The timestamp of the command execution. Default is None.
            add_params_to_data: If true, add context parameters to data

        Returns:
            HistoryEntry: A `HistoryEntry` instance representing the command history entry.
        """
        command: str
        current: Context = ctx
        command_parts: list[str] = []
        while current:
            command_parts.insert(0, current.command.name)
            current = current.parent
        command = ".".join(command_parts)

        operation = f"{command.strip(':.')}:{operation.strip(':')}"

        if add_params_to_data and isinstance(data, dict):
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
