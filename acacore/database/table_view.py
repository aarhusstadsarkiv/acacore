from sqlite3 import Connection
from typing import Generator
from typing import Generic
from typing import Self
from typing import Type
from typing import TypeVar

from pydantic import BaseModel

from .column import SQLValue
from .cursor import Cursor
from .table import _Where
from .table import Table

M = TypeVar("M", bound=BaseModel)


class View(Generic[M]):
    def __init__(
        self,
        database: Connection,
        model: Type[M],
        name: str,
        select: str,
        ignore: list[str] | None = None,
    ) -> None:
        self.database: Connection = database
        self.model: Type[M] = model
        self.name: str = name
        self.select_stmt: str = select
        self._table: Table[M] = Table(self.database, self.model, self.name, ignore=ignore)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r}, {self.model.__name__})"

    def __iter__(self) -> Generator[M, None, None]:
        yield from self.select()

    def __len__(self) -> int:
        return len(self._table)

    def __getitem__(self, where: _Where | M) -> M | None:
        return self._table.select(where, limit=1).fetchone()

    def __contains__(self, where: M) -> bool:
        return self._table.select(where, limit=1).cursor.fetchone() is not None

    def create_sql(self, *, exist_ok: bool = False) -> str:
        return f"create view {'if not exists' if exist_ok else ''} {self.name} as {self.select_stmt}"

    def create(self, *, exist_ok: bool = False) -> Self:
        self.database.execute(self.create_sql(exist_ok=exist_ok))
        return self

    def select(
        self,
        where: _Where | None = None,
        params: list[SQLValue] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Cursor[M]:
        return self._table.select(where, params, order_by, limit, offset)
