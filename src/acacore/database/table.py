from collections.abc import Generator
from re import sub
from sqlite3 import Connection
from sqlite3 import ProgrammingError
from typing import Literal
from typing import Self

from pydantic import BaseModel

from .column import ColumnSpec
from .column import SQLValue
from .cursor import Cursor

WhereDict = dict[str, SQLValue | list[SQLValue]]


def _where_dict_to_sql(where: WhereDict) -> tuple[str, list[SQLValue]]:
    """
    Convert a where statement in dict format into an SQL string and parameters list.

    :param where: The statement to convert a dictionary containing column names and
        values. Each value can be a single object or a list, if it is the latter the values will be matched with "OR".
    :return: A tuple containing the SQL where expression as a string, and a list of parameters converted to
        SQLite-compatible types
    """
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
    where: str | WhereDict | BaseModel,
    params: list[SQLValue] | None,
    primary_keys: list[ColumnSpec],
) -> tuple[str, list[SQLValue]]:
    """
    Turn a where statement/dict/model into an SQL string and parameters list.

    :param where: The statement to convert. This can be a string, a dictionary containing column names and
        values, or an instance of the model used by the table if primary keys have been defined.
    :param params: The parameters for the where statement, only used if ``where`` is a string.
    :param primary_keys: The primary keys of the table, only used if ``where`` is a ``BaseModel`` instance.
    :raise TypeError: If ``where`` is anything but a string, a dict, or a ``BaseModel`` instance.
    :return: A tuple containing the SQL where expression as a string, and a list of parameters converted to
        SQLite-compatible types
    """
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


class Table[M: BaseModel]:
    """
    A class that represents a table in an SQLite database and allows accessing rows as Pydantic models.

    :ivar database: The connection to the database.
    :ivar model: The model the table is based on.
    :ivar name: The name of the table.
    :ivar columns: The columns in the table as a dictionary of name keys and ``ColumnSpec`` values.
    """

    def __init__(
        self,
        database: Connection,
        model: type[M],
        name: str,
        primary_keys: list[str] | None = None,
        indices: dict[str, list[str]] | None = None,
        ignore: list[str] | None = None,
    ) -> None:
        """
        :param database: The connection to the database.
        :param model: The Pydantic model to create the table for.
        :param name: The name of the table.
        :param primary_keys: The primary keys of the table.
        :param indices: The indices of the table as index in the form {index name: list of indexed columns}.
        :param ignore: A list of field names to ignore from the model.
        """  # noqa: D205
        self.database: Connection = database
        self.model: type[M] = model
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

    def __len__(self) -> int:
        return self.count()

    def __getitem__(self, where: str | WhereDict | M) -> M | None:
        return self.select(where, limit=1).fetchone()

    def __setitem__(self, where: str | WhereDict | M | slice, row: M) -> None:
        if isinstance(where, slice):
            self.insert(row)
        else:
            self.update(row, where)

    def __delitem__(self, where: str | WhereDict | M) -> None:
        self.delete(where)

    def __contains__(self, where: M) -> bool:
        return self.select(where, limit=1).cursor.fetchone() is not None

    def create_sql(self, *, temporary: bool = False, exist_ok: bool = False) -> str:
        """
        Generate the SQL statement to create the table.

        :param temporary: Whether the table should be temporary (removed when connection closes) or not.
        :param exist_ok: Whether to ignore any existing table with the same name.
        """
        sql: list[str] = [f"create {'temporary' if temporary else ''} table"]

        if exist_ok:
            sql.append("if not exists")

        sql.append(self.name)

        sql_cols = [c.spec_sql() for c in self.columns.values()]
        if self.primary_keys:
            sql_cols.append(f"primary key ({','.join(pk.name for pk in self.primary_keys)})")

        sql.append(f"({','.join(sql_cols)})")

        return " ".join(sql)

    def indices_sql(self, *, exist_ok: bool = False) -> list[str]:
        """Generate the SQL statements to create the tables' indices."""
        return [
            f"create index {'if not exists' if exist_ok else ''} idx_{self.name}_{index} on {self.name} ({','.join(c.name for c in cols)})"
            for index, cols in self.indices.items()
        ]

    def create(self, *, temporary: bool = False, exist_ok: bool = False) -> Self:
        """
        Create the table in the connected database.

        :param temporary: Whether the table should be temporary (removed when connection closes) or not.
        :param exist_ok: Whether to ignore any existing table with the same name.
        """
        self.database.execute(self.create_sql(temporary=temporary, exist_ok=exist_ok))
        for index_sql in self.indices_sql(exist_ok=exist_ok):
            self.database.execute(index_sql)
        return self

    def drop(self, *, missing_ok: bool = True):
        """
        Drop the table in the connected database.

        :param missing_ok: Whether to accept that the table is missing or not.
        """
        self.database.execute(f"drop table {'if exists' if missing_ok else ''} {self.name}")

    def select(
        self,
        where: str | WhereDict | M | None = None,
        params: list[SQLValue] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Cursor[M]:
        """
        Select entries from the table.

        :param where: The where statement to use. This can be a string, a dictionary containing column names and
            values, or an instance of the model used by the table if primary keys have been defined.
        :param params: The parameters to use for the query, they are ignored if the ``where`` argument is anything but
            a string.
        :param order_by: A list of column names and direction ("asc", "desc") tuples to sort the results.
        :param limit: The maximum number of results to return.
        :param offset: The offset to start the results from.
        :return: A ``Cursor`` instance.
        """
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

        return Cursor(
            self.database.execute(" ".join(sql), params),
            self.model,
            list(self.columns.values()),
        )

    def count(
        self,
        where: str | WhereDict | M | None = None,
        params: list[SQLValue] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> int:
        """
        Count entries from the table.

        :param where: The where statement to use. This can be a string, a dictionary containing column names and
            values, or an instance of the model used by the table if primary keys have been defined.
        :param params: The parameters to use for the query, they are ignored if the ``where`` argument is anything but
            a string.
        :param limit: The maximum number of results to return.
        :param offset: The offset to start the results from.
        :return: A ``Cursor`` instance.
        """
        where, params = _where_to_sql(where, params, self.primary_keys)

        sql: list[str] = [f"select count(*) from {self.name}"]

        if where:
            sql.append(f"where {where}")

        if limit is not None:
            sql.append(f"limit {limit}")
        if offset is not None:
            sql.append(f"offset {offset}")

        return self.database.execute(" ".join(sql), params).fetchone()[0]

    def insert(self, *rows: M, on_exists: Literal["ignore", "replace", "error"] = "error") -> int:
        """
        Insert entries into the table.

        :param rows: The objects to insert.
        :param on_exists: What to do if the object exists: "ignore" to ignore any existing entries, "replace" to
            replace any existing entry, "error" to raise an error. Defaults to "error".
        :return: The number of inserted rows.
        """
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
        """
        Insert entries into the table or update existing entries.

        :param rows: The objects to upsert.
        :return: The number of inserted/modified rows.
        """
        return self.insert(*rows, on_exists="replace")

    def update(self, row: M, where: str | WhereDict | M = None, params: list[SQLValue] | None = None) -> int:
        """
        Update a single entry in the table.

        :param row: The object to update.
        :param where: Optionally, a where statement to specify which row(s) should be updated, uses ``row`` by default.
        :param params: The parameters to use for ``where``, if given.
        :return: The number of modified rows.
        """
        where, params = _where_to_sql(where or row, params, self.primary_keys)

        if not where:
            raise ProgrammingError("Update without where")

        cols: list[ColumnSpec] = list(self.columns.values())

        return self.database.execute(
            f"update {self.name} set {','.join(f'{c.name} = ?' for c in cols)} where {where}",
            [*[c.to_sql(getattr(row, c.name)) for c in cols], *params],
        ).rowcount

    def delete(self, where: str | WhereDict | M) -> int:
        """
        Delete rows from the table.

        :param where: The where statement to use. This can be a string, a dictionary containing column names and
            values, or an instance of the model used by the table if primary keys have been defined.
        :raise ProgrammingError: If ``where`` is empty.
        :return: The number of deleted rows.
        """
        where, params = _where_to_sql(where, [], self.primary_keys)

        if not where:
            raise ProgrammingError("Delete without where")

        return self.database.execute(f"delete from {self.name} where {where}", params).rowcount
