from re import sub
from sqlite3 import Connection
from sqlite3 import ProgrammingError
from typing import Generator
from typing import Generic
from typing import Literal
from typing import Self
from typing import Type
from typing import TypeAlias
from typing import TypeVar

from pydantic import BaseModel

from .column import ColumnSpec
from .column import SQLValue
from .cursor import Cursor

M = TypeVar("M", bound=BaseModel)
_Where: TypeAlias = str | dict[str, SQLValue | list[SQLValue]]


def _where_dict_to_sql(where: dict[str, SQLValue | list[SQLValue]]) -> tuple[str, list[SQLValue]]:
    params: list[SQLValue] = []
    sql: list[str] = []

    for k, vs in where.items():
        vs = vs if isinstance(vs, list) else [vs]
        col_sql: list[str] = []

        for v in vs:
            if v is None:
                col_sql.append(f"{k} is null")
            else:
                col_sql.append(f"{k} = ?")
                params.append(v)

        if len(col_sql) == 1:
            sql.append(col_sql[0])
        elif col_sql:
            sql.append(f"({' or '.join(col_sql)})")

    return " and ".join(sql).strip(), params


def _where_to_sql(
    where: _Where | BaseModel,
    params: list[SQLValue] | None,
    primary_keys: list[ColumnSpec],
) -> tuple[str, list[SQLValue]]:
    params = params or []

    if where is None:
        where = ""
    elif isinstance(where, BaseModel):
        where, params = _where_dict_to_sql({pk.name: pk.to_sql(getattr(where, pk.name)) for pk in primary_keys})
    elif isinstance(where, str):
        where = sub(r"^where\s+", "", where) if where.strip() else ""
    elif isinstance(where, dict):
        where, params = _where_dict_to_sql(where)
    else:
        raise TypeError(f"Unsupported type {type(where)}")

    return where.strip(), params if where else []


class Table(Generic[M]):
    def __init__(
        self,
        database: Connection,
        model: Type[M],
        name: str,
        primary_keys: list[str] | None = None,
        indices: dict[str, list[str]] | None = None,
        ignore: list[str] | None = None,
    ) -> None:
        self.database: Connection = database
        self.model: Type[M] = model
        self.name: str = name
        self.columns: dict[str, ColumnSpec] = {c.name: c for c in ColumnSpec.from_model(self.model, ignore)}

        _primary_keys: set[str] = set(primary_keys or [])
        _indices: dict[str, set[str]] = {_i: set(cs) for i, cs in (indices or {}).items() if (_i := i.strip())}

        if missing_keys := [pk for pk in _primary_keys if pk not in self.columns]:
            raise ValueError(
                f"Primary keys {', '.join(map(repr, missing_keys))} do not exist in model {self.model.__name__!r}"
            )

        if missing_keys := [c for cs in _indices.values() for c in cs if c not in self.columns]:
            raise ValueError(
                f"Index keys {', '.join(map(repr, missing_keys))} do not exist in model {self.model.__name__!r}"
            )

        self.primary_keys: list[ColumnSpec] = [self.columns[pk] for pk in _primary_keys]
        self.indices: dict[str, list[ColumnSpec]] = {i: [self.columns[c] for c in cs] for i, cs in _indices.items()}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r}, {self.model.__name__})"

    def __iter__(self) -> Generator[M, None, None]:
        yield from self.select()

    def __getitem__(self, where: _Where | M) -> M | None:
        return self.select(where, limit=1).fetchone()

    def __setitem__(self, where: _Where | M | slice, row: M) -> None:
        if isinstance(where, slice):
            self.insert(row)
        else:
            self.update(row, where)

    def __delitem__(self, where: _Where | M) -> None:
        self.delete(where)

    def __contains__(self, where: M) -> bool:
        return self.select(where, limit=1).cursor.fetchone() is not None

    def create_sql(self, *, exist_ok: bool = False) -> str:
        sql: list[str] = ["create table"]

        if exist_ok:
            sql.append("if not exists")

        sql.append(self.name)

        sql_cols = [c.spec_sql() for c in self.columns.values()]
        if self.primary_keys:
            sql_cols.append(f"primary key ({','.join(pk.name for pk in self.primary_keys)})")

        sql.append(f"({','.join(sql_cols)})")

        return " ".join(sql)

    def indices_sql(self, *, exist_ok: bool = False) -> list[str]:
        return [
            f"create index {'if not exists' if exist_ok else ''} idx_{self.name}_{index} on {self.name} ({','.join(c.name for c in cols)})"
            for index, cols in self.indices.items()
        ]

    def create(self, *, exist_ok: bool = False) -> Self:
        self.database.execute(self.create_sql(exist_ok=exist_ok))
        for index_sql in self.indices_sql(exist_ok=exist_ok):
            self.database.execute(index_sql)
        return self

    def select(
        self,
        where: _Where | M | None = None,
        params: list[SQLValue] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Cursor[M]:
        where, params = _where_to_sql(where, params, self.primary_keys)

        sql: list[str] = [f"select * from {self.name}"]

        if where:
            sql.append(f"where {where}")

        if order_by:
            sql.append(f"order by {','.join(o + ' ' + d for o, d in order_by)}")

        if limit is not None:
            sql.append(f"limit {limit}")
        if offset is not None:
            sql.append(f"offset {offset}")

        return Cursor(self.database.execute(" ".join(sql), params), self.model, list(self.columns.values()))

    def insert(self, *rows: M, on_exists: Literal["ignore", "replace", "error"] = "error") -> int:
        cols: list[ColumnSpec] = list(self.columns.values())
        sql: list[str] = ["insert"]

        if on_exists in ("ignore", "replace"):
            sql.append(f"or {on_exists}")

        sql.append(f"into {self.name}")

        sql.append(f"({','.join(c.name for c in cols)}) values ({','.join('?' * len(cols))})")

        return self.database.executemany(
            " ".join(sql),
            (tuple(c.to_sql(getattr(row, c.name)) for c in cols) for row in rows),
        ).rowcount

    def upsert(self, *rows: M) -> int:
        return self.insert(*rows, on_exists="replace")

    def update(self, row: M, where: _Where | M = None, params: list[SQLValue] | None = None) -> int:
        where, params = _where_to_sql(where or row, params, self.primary_keys)

        if not where:
            raise ProgrammingError("Update without where")

        cols: list[ColumnSpec] = list(self.columns.values())

        return self.database.execute(
            f"update {self.name} set {','.join(f'{c.name} = ?' for c in cols)} where {where}",
            [*[c.to_sql(getattr(row, c.name)) for c in cols], *params],
        ).rowcount

    def delete(self, where: _Where | M) -> int:
        where, params = _where_to_sql(where, [], self.primary_keys)

        if not where:
            raise ProgrammingError("Delete without where")

        return self.database.execute(f"delete from {self.name} where {where}", params).rowcount
