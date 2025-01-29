from collections.abc import Generator
from sqlite3 import Connection
from typing import Any
from typing import Generic
from typing import overload

from pydantic import BaseModel

from .table import _M
from .table import Table


class KeysTableModel(BaseModel):
    """The model for the rows in a key-value store table."""

    key: str
    value: object | None


class KeysTable(Generic[_M]):
    """
    A class that represents a key-value store table in an SQLite database and allows accessing the contents with a Pydantic model.

    :ivar table: The underlying ``Table`` object that handles insert/read/update/delete operations for each field.
    :ivar model: The model the table is based on.
    """

    def __init__(self, database: Connection, model: type[_M], name: str) -> None:
        """
        :param database: The connection to the database.
        :param model: The Pydantic model to create the table for.
        :param name: The name of the table.
        """  # noqa: D205
        self.table: Table[KeysTableModel] = Table(database, KeysTableModel, name, ["key"])
        self.model: type[_M] = model

    @property
    def name(self) -> str:
        """The name of the table."""
        return self.table.name

    @name.setter
    def name(self, value: str):
        self.table.name = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.table.name!r}, {self.model.__name__})"

    def __iter__(self) -> Generator[tuple[str, object | None], None, None]:
        yield from ((kv.key, kv.value) for kv in self.table.select())

    def __getitem__(self, key: str) -> Any | None:  # noqa: ANN401
        return self.get(key)

    def __setitem__(self, key: str, value: object | None) -> None:
        if key not in self.model.model_fields:
            raise AttributeError(f"{self.model.__name__!r} object has no attribute {key!r}")

        self.table.insert(KeysTableModel(key=key, value=value), on_exists="replace")

    def create_sql(self, *, exist_ok: bool = False) -> str:
        """Generate the SQL statement to create the table."""
        return self.table.create_sql(exist_ok=exist_ok)

    def create(self, *, exist_ok: bool = False):
        """
        Create the table in the connected database.

        :param exist_ok: Whether to ignore any existing table with the same name.
        """
        self.table.create(exist_ok=exist_ok)
        return self

    def set(self, obj: _M):
        """
        Save an object into the table.

        :param obj: The object to be saved.
        """
        self.table.insert(
            *(KeysTableModel(key=k, value=o) for k, o in obj.model_dump().items()),
            on_exists="replace",
        )

    @overload
    def get(self) -> _M | None: ...

    @overload
    def get(self, key: str) -> Any | None: ...  # noqa: ANN401

    def get(self, key: str | None = None) -> _M | Any | None:
        """
        Get the object stored in the table.

        :param key: If given, return only the value of that field.
        :return: The object stored in the table.
        """
        if key is not None and key not in self.model.model_fields:
            raise AttributeError(f"{self.model.__name__!r} object has no attribute {key!r}")

        items = self.table.select().fetchall()
        if not items:
            return None
        obj = self.model.model_validate({i.key: i.value for i in items})
        if key is not None:
            return getattr(obj, key)
        else:
            return obj

    def update(self, **kwargs: object | None):
        """
        Updated specific fields of the object stored in the table.

        :return: The number of updated fields.
        """
        if missing_keys := [k for k in kwargs if k not in self.model.model_fields]:
            raise AttributeError(
                f"Fields {', '.join(map(repr, missing_keys))} do not exist in model {self.model.__name__!r}"
            )
        return self.table.insert(
            *(KeysTableModel(key=k, value=o) for k, o in kwargs.items()),
            on_exists="replace",
        )
