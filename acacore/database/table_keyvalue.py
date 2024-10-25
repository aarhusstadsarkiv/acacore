from sqlite3 import Connection
from typing import Any
from typing import Generator
from typing import Generic
from typing import overload
from typing import Type

from pydantic import BaseModel

from .table import _M
from .table import Table


class KeysTableModel(BaseModel):
    key: str
    value: object | None


class KeysTable(Generic[_M]):
    def __init__(self, database: Connection, model: Type[_M], name: str) -> None:
        self.table: Table[KeysTableModel] = Table(database, KeysTableModel, name)
        self.model: Type[_M] = model

    @property
    def name(self) -> str:
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
        return self.table.create_sql(exist_ok=exist_ok)

    def create(self, *, exist_ok: bool = False):
        self.table.create(exist_ok=exist_ok)
        return self

    def set(self, obj: _M):
        self.table.insert(*(KeysTableModel(key=k, value=o) for k, o in obj.model_dump().items()), on_exists="replace")

    @overload
    def get(self) -> _M | None: ...

    @overload
    def get(self, key: str) -> Any | None: ...  # noqa: ANN401

    def get(self, key: str | None = None) -> _M | Any | None:
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
        if missing_keys := [k for k in kwargs if k not in self.model.model_fields]:
            raise AttributeError(
                f"Fields {', '.join(map(repr, missing_keys))} do not exist in model {self.model.__name__!r}"
            )
        return self.table.insert(*(KeysTableModel(key=k, value=o) for k, o in kwargs.items()), on_exists="replace")
