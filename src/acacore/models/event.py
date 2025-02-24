from collections.abc import Sequence
from datetime import datetime
from logging import Logger
from typing import Any
from typing import Literal
from typing import Self
from uuid import UUID

from click import Context
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
from pydantic import UUID4

from acacore.__version__ import __version__


class Event(BaseModel):
    file_uuid: UUID4 | None = None
    file_type: Literal["original", "master", "access", "statutory"] | None = None
    time: datetime = Field(default_factory=datetime.now)
    operation: str
    data: object | None = None
    reason: str | None = None

    @classmethod
    @model_validator(mode="after")
    def _model_validator(cls, data: Self):
        if (data.file_uuid and not data.file_type) or (not data.file_uuid and data.file_type):
            raise ValueError("uuid and file type must be set together")
        return data

    @classmethod
    def from_command(
        cls,
        ctx: Context | str,
        operation: str,
        file: tuple[UUID, Literal["original", "master", "access", "statutory"]] | None = None,
        data: object | None = None,
        reason: str | None = None,
        time: datetime | None = None,
        add_params_to_data: bool = False,
    ) -> Self:
        """
        Create an Event for a command.

        :param ctx: The context object representing the current command execution.
        :param operation: The name of the operation for which the command history entry is being created.
        :param file: Optional. The UUID and file type (original or master) associated with the command history entry,
            defaults to None.
        :param data: Optional. Additional data or parameters associated with the command history entry.
        :param reason: Optional. The reason for the command execution, defaults to None.
        :param time: Optional. The timestamp of the command execution, defaults to None.
        :param add_params_to_data: If true, add context parameters to data, defaults to False.
        :return: An `Event` instance representing the command history entry.
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
            file_uuid=file[0] if file else None,
            file_type=file[1] if file else None,
            time=time or datetime.now(),
            operation=operation,
            data=data,
            reason=reason,
        )

    def log(
        self,
        level: int,
        *logger: Logger,
        show_null: bool = False,
        show_args: bool | Sequence[str] = True,
        **extra: Any,  # noqa: ANN401
    ):
        """
        Log the event with the given loggers.

        The message uses the format ``{operation} uuid={uuid} data={data} reason={reason}``.
        All ``extra`` arguments are added with the format ``{key}={value}``.

        :param level: The logging level to be used for the log message.
        :param logger: The logger(s) to which the log message will be sent.
        :param show_null: Flag indicating whether to include null values in the log message. Default is False.
        :param show_args: Set to true to show all arguments (uuid, data, reason) in the log message, or a list of
            argument names to show only specific ones. Default is True.
        :param extra: Additional arguments to be shown in the log message.
        """
        uuid_msg: str | None = f"{self.file_type}:{self.file_uuid}" if self.file_uuid else None

        if not show_args:
            msg: str = self.operation
        elif show_args is True and show_null:
            msg: str = f"{self.operation} uuid={uuid_msg} data={self.data} reason={self.reason}"
        elif show_args is True:
            msg: str = (
                f"{self.operation}"
                + (f" uuid={uuid_msg}" if self.file_uuid is not None else "")
                + (f" data={self.data}" if self.data is not None else "")
                + (f" reason={self.reason.strip()}" if self.reason is not None else "")
            )
        else:
            msg: str = (
                f"{self.operation}"
                + (f" uuid={uuid_msg}" if "uuid" in show_args else "")
                + (f" data={self.data}" if "data" in show_args else "")
                + (f" reason={self.reason.strip()}" if "reason" in show_args else "")
            )

        for keyword, value in extra.items():
            msg += f" {keyword.strip()}={value}"

        for logger in logger:
            logger.log(level, msg)
