from datetime import datetime
from typing import Optional
from typing import Union

from click import Context
from pydantic import Field
from pydantic import UUID4

from .base import ACABase


class HistoryEntry(ACABase):
    uuid: Optional[UUID4] = Field(default=None, index=["idx_uuid_history"])
    time: datetime = Field(default_factory=datetime.now)
    operation: str
    data: Optional[object] = None
    reason: Optional[str] = None

    @classmethod
    def command_history(
        cls,
        ctx: Union[Context, str],
        operation: str,
        uuid: Optional[UUID4] = None,
        data: Optional[object] = ...,
        reason: Optional[str] = None,
        time: Optional[datetime] = None,
    ) -> "HistoryEntry":
        """
        Create a HistoryEntry for a command.

        Args:
            ctx: The context object representing the current command execution. Can be either a `click.Context` instance or a string.
            operation: The name of the operation for which the command history entry is being created.
            uuid: Optional. The UUID associated with the command history entry. Default is None.
            data: Optional. Additional data or parameters associated with the command history entry.
                It defaults to the params property of the context, if it is a click.Context object, otherwise None.
            reason: Optional. The reason for the command execution. Default is None.
            time: Optional. The timestamp of the command execution. Default is None.

        Returns:
            HistoryEntry: A `HistoryEntry` instance representing the command history entry.
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

        if data is Ellipsis:
            data = ctx.params if isinstance(ctx, Context) else None

        return cls(
            uuid=uuid,
            time=time or datetime.now(),  # noqa: DTZ005
            operation=operation,
            data=data,
            reason=reason,
        )
