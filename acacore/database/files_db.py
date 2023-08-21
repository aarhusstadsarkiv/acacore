from datetime import datetime
from os import PathLike
from pathlib import Path
from sqlite3 import Connection, Cursor as SQLiteCursor, OperationalError
from typing import Optional, Type, Callable, Union, TypeVar, Generic, Generator, Any, overload
from uuid import UUID

from pydantic.main import BaseModel

T = TypeVar("T")
M = TypeVar("M", bound=BaseModel)


class Cursor:
    def __init__(self, cursor: SQLiteCursor, columns: list[Union['Column', 'SelectColumn']],
                 table: Optional['Table'] = None):
        self.cursor: SQLiteCursor = cursor
        self.columns: list[Union['Column', 'SelectColumn']] = columns
        self.table: Optional[Table] = table

    def __iter__(self) -> Generator[dict[str, Any], None, None]:
        return self.fetchall()

    def fetchalltuples(self) -> Generator[tuple, None, None]:
        return (
            tuple(c.from_entry(v) for c, v in zip(self.columns, vs, strict=True))
            for vs in self.cursor.fetchall()
        )

    def fetchonetuple(self) -> Optional[tuple]:
        vs: tuple = self.cursor.fetchone()

        return tuple(c.from_entry(v) for c, v in zip(self.columns, vs, strict=True)) if vs else None

    @overload
    def fetchall(self) -> Generator[dict[str, Any], None, None]:
        ...

    @overload
    def fetchall(self, model: Type[M]) -> Generator[M, None, None]:
        ...

    def fetchall(self, model: Optional[Type[M]] = None) -> Generator[Union[dict[str, Any], M], None, None]:
        select_columns: list[SelectColumn] = [SelectColumn.from_column(c) for c in self.columns]

        if model:
            return (
                model.model_validate({
                    c.alias or c.name: v
                    for c, v in zip(select_columns, vs, strict=True)
                })
                for vs in self.cursor.fetchall()
            )

        return (
            {
                c.alias or c.name: c.from_entry(v)
                for c, v in zip(select_columns, vs, strict=True)
            }
            for vs in self.cursor.fetchall()
        )

    @overload
    def fetchone(self) -> Generator[dict[str, Any], None, None]:
        ...

    @overload
    def fetchone(self, model: Type[M]) -> Generator[M, None, None]:
        ...

    def fetchone(self, model: Optional[Type[M]] = None) -> Optional[Union[dict[str, Any], M]]:
        select_columns: list[SelectColumn] = [SelectColumn.from_column(c) for c in self.columns]
        vs: tuple = self.cursor.fetchone()

        if vs is None:
            return None

        entry: dict[str, Any] = {
            c.name: c.from_entry(v)
            for c, v in zip(select_columns, vs, strict=True)
        }

        return model.model_validate(entry) if model else entry


class Column(Generic[T]):
    def __init__(self, name: str, sql_type: str,
                 to_entry: Callable[[T], Union[str, bytes, int, float, bool, datetime, None]],
                 from_entry: Callable[[Union[str, bytes, int, float, bool, datetime, None]], T],
                 unique: bool = False, primary_key: bool = False, not_null: bool = False,
                 check: Optional[str] = None, default: Optional[T] = ...):
        self.name: str = name
        self.sql_type: str = sql_type
        self.to_entry: Callable[[T], Union[str, bytes, int, float, bool, datetime, None]] = to_entry
        self.from_entry: Callable[[Union[str, bytes, int, float, bool, datetime, None]], T] = from_entry
        self.unique: bool = unique
        self.primary_key: bool = primary_key
        self.not_null: bool = not_null
        self._check: str = check or ""
        self.default: Union[Optional[T], Ellipsis] = default

    @property
    def check(self) -> str:
        return self._check.format(name=self.name) if self._check else ""

    @check.setter
    def check(self, check: Optional[str]):
        self._check = check

    def create_statement(self) -> str:
        elements: list[str] = [self.name, self.sql_type]
        if self.unique:
            elements.append("unique")
        if self.not_null:
            elements.append("not null")
        if self.default is not Ellipsis:
            elements.append(f"default {self.to_entry(self.default)}")
        if self.check:
            elements.append(f"check ({self.check})")

        return " ".join(elements)


class SelectColumn(Column):
    def __init__(self, name: str, from_entry: Callable[[Union[str, bytes, int, float, bool, datetime, None]], T],
                 alias: Optional[str] = None):
        super().__init__(name, "", lambda x: x, from_entry)
        self.alias: Optional[str] = alias

    @classmethod
    def from_column(cls, column: 'Column', alias: Optional[str] = None) -> 'SelectColumn':
        select_column = SelectColumn(column.name, column.from_entry, alias)
        select_column.sql_type = column.sql_type
        select_column.to_entry = column.to_entry
        select_column.unique = column.unique
        select_column.primary_key = column.primary_key
        select_column.not_null = column.not_null
        select_column._check = column._check

        if isinstance(column, SelectColumn):
            select_column.alias = alias or column.alias

        return select_column


# noinspection SqlNoDataSourceInspection
class Table:
    def __init__(self, connection: 'FileDB', name: str, columns: list[Column]):
        self.connection: 'FileDB' = connection
        self.name: str = name
        self.columns: list[Column] = columns

    def __len__(self) -> int:
        return self.connection.execute(f"select count(*) from {self.name}").fetchone()[0]

    def __iter__(self) -> Generator[dict[str, Any], None, None]:
        return self.select().fetchall()

    @property
    def keys(self) -> list[Column]:
        return [c for c in self.columns if c.primary_key]

    @property
    def create_statement(self, exist_ok: bool = True) -> str:
        elements: list[str] = ["create table"]

        if exist_ok:
            elements.append("if not exists")

        elements.append(self.name)

        if self.columns:
            elements.append(
                "(" +
                ", ".join(c.create_statement() for c in self.columns) +
                (f", primary key ({', '.join(c.name for c in keys)})" if (keys := self.keys) else "") +
                ")"
            )

        return " ".join(elements)

    def select(self, columns: Optional[list[Union['Column', 'SelectColumn']]] = None,
               where: Optional[str] = None,
               order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
               limit: Optional[int] = None,
               parameters: Optional[list[Any]] = None) -> Cursor:
        columns = columns or self.columns
        parameters = parameters or []

        assert columns, "Columns cannot be empty"

        select_columns: list[SelectColumn] = [SelectColumn.from_column(c) for c in columns]

        select_names = [
            '{} as {}'.format(c.name, c.alias) if c.alias else c.name
            for c in select_columns
        ]

        statement: str = f"SELECT {','.join(select_names)} FROM {self.name}"

        if where:
            statement += f" WHERE {where}"

        if order_by:
            order_statements = [
                f"{c.name if isinstance(c, Column) else c} {s}"
                for c, s in order_by
            ]
            statement += f" ORDER BY {','.join(order_statements)}"

        if limit is not None:
            statement += f" LIMIT {limit}"

        return Cursor(self.connection.execute(statement, parameters), columns, self)

    def insert(self, entry: dict[str, Any], exist_ok: bool = False, replace: bool = False):
        values: list[Union[str, bytes, int, float, bool, datetime, None]] = [
            c.to_entry(entry[c.name]) for c in self.columns
        ]

        elements: list[str] = ["INSERT"]

        if replace:
            elements.append("OR REPLACE")
        elif exist_ok:
            elements.append("OR IGNORE")

        elements.append(f"INTO {self.name}")

        elements.append(f"({','.join(c.name for c in self.columns)})")
        elements.append(f"VALUES ({','.join('?' * len(values))})")

        self.connection.execute(" ".join(elements), values)


# noinspection SqlNoDataSourceInspection
class View(Table):
    def __init__(self, connection: 'FileDB', name: str, on: Union[Table, str],
                 columns: list[Union[Column, SelectColumn]], where: Optional[str] = None,
                 group_by: Optional[list[Union[Column, SelectColumn]]] = None,
                 order_by: Optional[list[tuple[Union[str, Column], str]]] = None, limit: Optional[int] = None):
        assert columns, "Views must have columns"
        super().__init__(connection, name, columns)
        self.on: Union[Table, str] = on
        self.where: str = where
        self.group_by: list[Union[Column, SelectColumn]] = group_by or []
        self.order_by: Optional[list[tuple[Union[str, Column], str]]] = order_by or []
        self.limit: Optional[int] = limit

    @property
    def create_statement(self, exist_ok: bool = True) -> str:
        elements: list[str] = ["CREATE VIEW"]

        if exist_ok:
            elements.append("IF NOT EXISTS")

        elements.append(self.name)

        elements.append("AS")

        select_names = [
            f'{c.name} as {c.alias}' if c.alias else c.name
            for c in [SelectColumn.from_column(c) for c in self.columns]
        ]

        elements.append(
            f"SELECT {','.join(select_names)} "
            f"FROM {self.on.name if isinstance(self.on, Table) else self.on}"
        )

        if self.where:
            elements.append(f"WHERE {self.where}")

        if self.group_by:
            elements.append("GROUP BY")
            elements.append(",".join([
                c.alias or c.name
                for c in [SelectColumn.from_column(c) for c in self.group_by]
            ]))

        if self.order_by:
            order_statements = [
                f"{(SelectColumn.from_column(c).name or c.name) if isinstance(c, Column) else c} {s}"
                for c, s in self.order_by
            ]
            elements.append(f"ORDER BY {','.join(order_statements)}")

        if self.limit is not None:
            elements.append(f"LIMIT {self.limit}")

        return " ".join(elements)

    def select(self, columns: Optional[list[Union['Column', 'SelectColumn']]] = None,
               where: Optional[str] = None,
               order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
               limit: Optional[int] = None,
               parameters: Optional[list[Any]] = None) -> Cursor:
        columns = columns or [
            Column(c.alias or c.name, c.sql_type, c.to_entry, c.from_entry, c.unique, c.primary_key, c.not_null,
                   c.check, c.default)
            for c in map(SelectColumn.from_column, self.columns)
        ]
        return super().select(columns, where, order_by, limit, parameters)

    def insert(self, *_args, **_kwargs):
        raise OperationalError("Cannot insert into view")


class FileDB(Connection):
    def __init__(self, database: str | bytes | PathLike[str] | PathLike[bytes], *,
                 timeout: float = 5.0,
                 detect_types: int = 0, isolation_level: Optional[str] = 'DEFERRED', check_same_thread: bool = True,
                 factory: Optional[Type[Connection]] = Connection, cached_statements: int = 100, uri: bool = False):
        super().__init__(database, timeout, detect_types, isolation_level, check_same_thread, factory,
                         cached_statements, uri)

        self.files = Table(self, "Files", [
            Column("id", "integer", int, int, True, True, True),
            Column("uuid", "varchar", str, UUID, True, False, True),
            Column("relative_path", "varchar", str, Path, False, False, True),
            Column("checksum", "varchar", lambda x: None if x is None else str(x),
                   lambda x: None if x is None else str(x), False, False, False),
            Column("puid", "varchar", lambda x: None if x is None else str(x), lambda x: None if x is None else str(x),
                   False, False, False),
            Column("signature", "varchar", str, str, False, False, False),
            Column("is_binary", "boolean", bool, lambda x: None if x is None else bool(x), False, False, False),
            Column("file_size_in_bytes", "integer", int, int, False, False, False),
            Column("warning", "varchar", str, str, False, False, False),
            Column("warning", "varchar", str, str, False, False, False),
        ])

        self.metadata = Table(self, "Metadata", [
            Column("last_run", "datetime", datetime.isoformat, datetime.fromisoformat, False, False, True),
            Column("processed_dir", "varchar", str, Path, False, False, True),
            Column("file_count", "integer", int, lambda x: None if x is None else bool(x), False, False, False),
            Column("total_size", "integer", int, lambda x: None if x is None else bool(x), False, False, False),
        ])

        self.identification_warnings = View(self, "_IdentificationWarnings", self.files, self.files.columns,
                                            f'"{self.files.name}".warning IS NOT null')

        self.signature_count = View(
            self, "_SignatureCount",
            self.files,
            [
                Column("puid", "varchar", str, str, False, False, False),
                Column("signature", "varchar", str, str, False, False, False),
                SelectColumn(
                    f'count('
                    f'CASE WHEN ("{self.files.name}".puid IS NULL) '
                    f'THEN \'None\' '
                    f'ELSE "{self.files.name}".puid '
                    f'END)',
                    int, "count")
            ],
            None,
            [
                Column("puid", "varchar", str, str, False, False, False),
            ],
            [
                (Column("count", "int", str, str), "ASC"),
            ])
