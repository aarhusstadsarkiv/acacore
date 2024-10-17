from collections.abc import Sequence
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor as SQLiteCursor
from typing import Iterable
from typing import Mapping
from typing import overload
from typing import Type
from typing import TypeAlias
from typing import TypeVar

from pydantic import BaseModel

from .column import SQLValue
from .table import Table
from .table_keyvalue import KeysTable
from .table_view import View

_M = TypeVar("_M", bound=BaseModel)
P: TypeAlias = Sequence[SQLValue] | Mapping[str, SQLValue]


class Database:
    def __init__(
        self,
        path: str | PathLike[str],
        *,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: str | None = "DEFERRED",
        check_same_thread: bool = True,
        cached_statements: int = 100,
    ) -> None:
        self.path: Path = Path(path)
        self.connection: Connection = Connection(
            self.path,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
            cached_statements=cached_statements,
        )
        self._committed_changes: int = 0

    @overload
    def execute(self, sql: str, /) -> SQLiteCursor: ...

    @overload
    def execute(self, sql: str, parameters: P, /) -> SQLiteCursor: ...

    def execute(self, sql: str, parameters: P | None = None, /) -> SQLiteCursor:
        return self.connection.execute(sql, parameters or [])

    def executemany(self, sql: str, parameters: Iterable[P], /) -> SQLiteCursor:
        return self.connection.executemany(sql, parameters)

    def commit(self):
        self.connection.commit()
        self._committed_changes = self.total_changes

    def rollback(self):
        self.connection.rollback()

    @property
    def total_changes(self):
        return self.connection.total_changes

    @property
    def committed_changes(self):
        return self._committed_changes

    @property
    def uncommitted_changes(self):
        return self.total_changes - self._committed_changes

    def close(self):
        self.connection.close()

    def create_table(
        self,
        model: Type[_M],
        name: str,
        primary_keys: list[str] | None = None,
        indices: dict[str, list[str]] | None = None,
        ignore: list[str] | None = None,
        *,
        exist_ok: bool = True,
    ) -> Table[_M]:
        return Table(self.connection, model, name, primary_keys, indices, ignore).create(exist_ok=exist_ok)

    def create_view(self, model: Type[_M], name: str, select: str, *, exist_ok: bool = True) -> View[_M]:
        return View(self.connection, model, name, select).create(exist_ok=exist_ok)

    def create_keys_table(self, model: Type[_M], name: str, *, exist_ok: bool = True) -> KeysTable[_M]:
        return KeysTable(self.connection, model, name).create(exist_ok=exist_ok)
