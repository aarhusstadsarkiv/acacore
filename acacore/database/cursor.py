from itertools import islice
from sqlite3 import Cursor as SQLiteCursor
from sqlite3 import Row
from typing import Any
from typing import Callable
from typing import Generator
from typing import Generic
from typing import Type
from typing import TypeVar

from pydantic import BaseModel

from .column import ColumnSpec
from .column import SQLValue

M = TypeVar("M", bound=BaseModel)


class Cursor(Generic[M]):
    def __init__(self, cursor: SQLiteCursor, model: Type[M], columns: list[ColumnSpec]) -> None:
        self.cursor: SQLiteCursor[Row] = cursor
        self.cursor.row_factory = Row
        self.model: Type[M] = model
        self.columns: list[ColumnSpec] = columns

        _cols: dict[str, Callable[[SQLValue], Any]] = {c.name: c.from_sql for c in columns}

        def _loader(row: Row) -> M:
            return self.model.model_validate({k: f(row[k]) for k, f in _cols.items()})

        self.entries: Generator[M, None, None] = (_loader(row) for row in self.cursor)

    def __iter__(self) -> Generator[M, None, None]:
        yield from self.entries

    def __next__(self) -> M:
        return next(self.entries)

    def fetchone(self) -> M | None:
        return next(self, None)

    def fetchmany(self, size: int) -> list[M]:
        return list(islice(self, size))

    def fetchall(self) -> list[M]:
        return list(self)
