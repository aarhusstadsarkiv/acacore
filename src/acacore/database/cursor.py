from collections.abc import Callable
from collections.abc import Generator
from itertools import islice
from sqlite3 import Cursor as SQLiteCursor
from sqlite3 import Row
from typing import Any
from typing import Generic
from typing import TypeVar

from pydantic import BaseModel

from .column import ColumnSpec
from .column import SQLValue

_M = TypeVar("_M", bound=BaseModel)


class Cursor(Generic[_M]):
    """
    Class that wraps arund an SQLite cursor to return Pydantic models instead of value tuples.

    :ivar cursor: The SQLite cursor
    :ivar model: The model to return data as.
    :ivar columns: A list of ``ColumnSpec`` instances that describe the columns in the cursor.
    """

    def __init__(self, cursor: SQLiteCursor, model: type[_M], columns: list[ColumnSpec]) -> None:
        """
        :param cursor: The SQLite cursor
        :param model: The model to return data as.
        :param columns: A list of ``ColumnSpec`` instances that describe the columns in the cursor.
        """  # noqa: D205
        self.cursor: SQLiteCursor[Row] = cursor
        self.cursor.row_factory = Row
        self.model: type[_M] = model
        self.columns: list[ColumnSpec] = columns
        self._cols: dict[str, Callable[[SQLValue], Any]] = {c.name: c.from_sql for c in columns}
        self._row: Callable[[Row], _M] = lambda r: self.model.model_validate(
            {k: f(r[k]) for k, f in self._cols.items()}
        )

    @property
    def rows(self) -> Generator[_M, None, None]:
        """The rows of the cursor as a generator."""
        return (self._row(row) for row in self.cursor)

    def __iter__(self) -> Generator[_M, None, None]:
        yield from self.rows

    def __next__(self) -> _M:
        return next(self.rows)

    def fetchone(self) -> _M | None:
        """Fetch the next row from the cursor, ``None`` if the cursor is exhausted."""
        return next(self.rows, None)

    def fetchmany(self, size: int) -> list[_M]:
        """Fetch the next ``size`` rows from the cursor."""
        return list(islice(self.rows, size))

    def fetchall(self) -> list[_M]:
        """Fetch all the rows from the cursor."""
        return list(self.rows)
