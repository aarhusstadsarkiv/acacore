from datetime import datetime
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor as SQLiteCursor
from sqlite3 import OperationalError
from typing import Any
from typing import Callable
from typing import Generator
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union
from typing import overload

from pydantic.main import BaseModel

T = TypeVar("T")
R = TypeVar("R")
M = TypeVar("M", bound=BaseModel)
V = Union[str, bytes, int, float, bool, datetime, None]

_sql_schema_types: dict[str, str] = {
    "string": "text",
    "integer": "integer",
    "number": "real",
    "boolean": "boolean",
    "bytes": "blob",
    "null": "text",
}


def _schema_to_column(name: str, schema: dict) -> 'Column':
    schema_type: Optional[str] = schema.get("type", None)
    schema_any_of: list[dict] = schema.get("anyOf", [])

    sql_type: str
    to_entry: Callable[[Optional[T]], V]
    from_entry: Callable[[V], Optional[T]]
    not_null: bool = (schema_any_of or [{}])[-1].get("type", None) != "null"

    if schema_type:
        sql_type = _sql_schema_types.get(schema_type, None)
        type_format: Optional[str] = schema.get("format", None)

        if type_format == "path":
            to_entry, from_entry = str, Path
        elif type_format == "date-time":
            to_entry, from_entry = datetime.isoformat, datetime.fromisoformat
        elif type_format == "binary":
            to_entry, from_entry = bytes, bytes
        elif schema_type == "string":
            to_entry, from_entry = str, str
        elif schema_type == "integer":
            to_entry, from_entry = float, float
        elif schema_type == "number":
            to_entry, from_entry = float, float
        elif schema_type == "boolean":
            to_entry, from_entry = bool, bool
        elif schema_type == "null":
            to_entry, from_entry = lambda x: x, lambda x: x
        else:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")
    elif schema_any_of:
        if len(schema_any_of) > 2:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")

        return _schema_to_column(name, {**schema_any_of[0], **schema})
    else:
        raise TypeError(f"Cannot recognize type from schema {schema!r}")

    return Column(
        name, sql_type, or_none(to_entry), or_none(from_entry),
        unique=schema.get("default", False),
        primary_key=schema.get("primary_key", False),
        not_null=not_null,
        default=schema.get("default", ...)
    )


def model_to_columns(model: Type[BaseModel]) -> list['Column']:
    return [_schema_to_column(p, s) for p, s in model.model_json_schema()["properties"].items()]


def or_none(func: Callable[[T], R]) -> Callable[[T], Optional[R]]:
    """
    Create a lambda function of arity one that will return None if its argument is None,
    otherwise it will call func on the object.

    Args:
        func: A function of type (T) -> R that will handle the object if it is not none.

    Returns:
        object: A function of type (T) -> R | None.
    """
    return lambda x: None if x is None else func(x)


class Cursor:
    def __init__(self, cursor: SQLiteCursor, columns: list[Union['Column', 'SelectColumn']],
                 table: Optional['Table'] = None):
        """
        A wrapper class for an SQLite cursor that returns its results as dicts (or objects).

        Args:
            cursor: An SQLite cursor from a select transaction.
            columns: A list of columns to use to convert the tuples returned by the cursor.
            table: Optionally, the Table from which on which the select transaction was executed.
        """
        self.cursor: SQLiteCursor = cursor
        self.columns: list[Union['Column', 'SelectColumn']] = columns
        self.table: Optional[Table] = table

    def __iter__(self) -> Generator[dict[str, Any], None, None]:
        return self.fetchall()

    def __next__(self) -> Optional[dict[str, Any]]:
        return self.fetchone()

    def fetchalltuples(self) -> Generator[tuple, None, None]:
        """
        Fetch all the results from the cursor as tuples and convert the data using the given columns.

        Returns:
            Generator: A generator for the tuples in the cursor.
        """
        return (
            tuple(c.from_entry(v) for c, v in zip(self.columns, vs, strict=True))
            for vs in self.cursor.fetchall()
        )

    def fetchonetuple(self) -> Optional[tuple]:
        """
        Fetch one result from the cursor as tuples and convert the data using the given columns.

        Returns:
            tuple: A single tuple from the cursor.
        """
        vs: tuple = self.cursor.fetchone()

        return tuple(c.from_entry(v) for c, v in zip(self.columns, vs, strict=True)) if vs else None

    @overload
    def fetchall(self) -> Generator[dict[str, Any], None, None]:
        ...

    @overload
    def fetchall(self, model: Type[M]) -> Generator[M, None, None]:
        ...

    def fetchall(self, model: Optional[Type[M]] = None) -> Generator[Union[dict[str, Any], M], None, None]:
        """
        Fetch all results from the cursor and return them as dicts, with the columns' names/aliases used as keys.

        Args:
            model: Optionally, a pydantic.BaseModel class to use instead of a dict.

        Returns:
            Generator: A generator for converted dicts (or models).
        """
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
    def fetchone(self) -> Optional[dict[str, Any]]:
        ...

    @overload
    def fetchone(self, model: Type[M]) -> Optional[M]:
        ...

    def fetchone(self, model: Optional[Type[M]] = None) -> Optional[Union[dict[str, Any], M]]:
        """
        Fetch one result from the cursor and return it as a dict, with the columns' names/aliases used as keys.

        Args:
            model: Optionally, a pydantic.BaseModel class to use instead of a dict.

        Returns:
            dict: A single dict (or model) if the cursor is not exhausted, otherwise None.
        """
        select_columns: list[SelectColumn] = [SelectColumn.from_column(c) for c in self.columns]
        vs: tuple = self.cursor.fetchone()

        if vs is None:
            return None

        entry: dict[str, Any] = {
            c.name: c.from_entry(v)
            for c, v in zip(select_columns, vs, strict=True)
        }

        return model.model_validate(entry) if model else entry


class ModelCursor(Cursor, Generic[M]):
    def __init__(self, cursor: SQLiteCursor, model: Type[M], table: Optional['Table'] = None):
        """
        A wrapper class for an SQLite cursor that returns its results as model objects.

        Args:
            cursor: An SQLite cursor from a select transaction.
            model: A model representing the objects in the cursor.
            table: Optionally, the Table from which on which the select transaction was executed.
        """
        super().__init__(cursor, model_to_columns(model), table)
        self.model: Type[M] = model

    def __iter__(self) -> Generator[M, None, None]:
        return self.fetchall()

    def __next__(self) -> Optional[M]:
        return self.fetchone()

    def fetchall(self, model: Optional[Type[M]] = None) -> Generator[M, None, None]:
        """
        Fetch all results from the cursor and return them as model objects.

        Args:
            model: Optionally, a different pydantic.BaseModel class to use instead of the one in the ModelCursor.

        Returns:
            Generator: A generator for converted objects.
        """
        return super().fetchall(model or self.model)

    def fetchone(self, model: Optional[Type[M]] = None) -> Optional[M]:
        """
        Fetch one result from the cursor and return it as model object.

        Args:
            model: Optionally, a different pydantic.BaseModel class to use instead of the one in the ModelCursor.

        Returns:
            object: A single object if the cursor is not exhausted, otherwise None.
        """
        return super().fetchone(model or self.model)


class Column(Generic[T]):
    def __init__(self, name: str, sql_type: str,
                 to_entry: Callable[[T], V], from_entry: Callable[[V], T],
                 unique: bool = False, primary_key: bool = False, not_null: bool = False,
                 check: Optional[str] = None, default: Optional[T] = ...):
        """
        A class that stores information regarding a table column.

        Args:
            name: The name of the column.
            sql_type: The SQL type to use when creating a table.
            to_entry: A function that returns a type supported by SQLite
                (str, bytes, int, float, bool, datetime, or None).
            from_entry: A function that takes a type returned by SQLite (str, bytes, int, float, or None)
                and returns another object.
            unique: True if the column should be set as UNIQUE.
            primary_key: True if the column is a PRIMARY KEY.
            not_null: True if the column is NOT NULL.
            check: A string containing a CHECK expression, {name} substrings will be substituted
                with the name of the column.
            default: The column's DEFAULT value, which will be converted using `to_entry`.
                Note that None is considered a valid default value; to set it to empty use Ellipsis (...).
        """
        self.name: str = name
        self.sql_type: str = sql_type
        self.to_entry: Callable[[T], V] = to_entry
        self.from_entry: Callable[[V], T] = from_entry
        self.unique: bool = unique
        self.primary_key: bool = primary_key
        self.not_null: bool = not_null
        self._check: str = check or ""
        self.default: Union[Optional[T], Ellipsis] = default

    def __repr__(self):
        return (f"{self.__class__.__name__}("
                f"{self.name}"
                f", {self.sql_type!r}"
                f", unique={self.unique}"
                f", primary_key={self.primary_key}"
                f", not_null={self.not_null}"
                f"{f', default={repr(self.default)}' if self.default is not Ellipsis else ''}"
                f")")

    @classmethod
    def from_model(cls, model: Type[BaseModel]) -> list['Column']:
        return model_to_columns(model)

    @property
    def check(self) -> str:
        return self._check.format(name=self.name) if self._check else ""

    @check.setter
    def check(self, check: Optional[str]):
        self._check = check

    def create_statement(self) -> str:
        """
        Generate the statement that creates the column.

        Returns:
            A column statement for a CREATE TABLE expression.
        """
        elements: list[str] = [self.name, self.sql_type]
        if self.unique:
            elements.append("unique")
        if self.not_null:
            elements.append("not null")
        if self.default is not Ellipsis:
            default_value: V = self.to_entry(self.default)
            elements.append(
                "default '{}'".format(default_value.replace("'", "\\'")) if isinstance(default_value, str)
                else f"default {'null' if default_value is None else default_value}"
            )
        if self.check:
            elements.append(f"check ({self.check})")

        return " ".join(elements)


class SelectColumn(Column):
    def __init__(self, name: str, from_entry: Callable[[V], T], alias: Optional[str] = None):
        """
        A subclass of Column for SELECT expressions that need complex statements and/or an alias.

        Args:
            name: The name or select statement for the select expression (e.g., count(*)).
            from_entry: A function that takes a type returned by SQLite (str, bytes, int, float, or None)
                and returns another object.
            alias: An alternative name for the select statement, it will be used with the AS keyword
                and as a key by Cursor.
        """
        super().__init__(name, "", lambda x: x, from_entry)
        self.alias: Optional[str] = alias

    @classmethod
    def from_column(cls, column: 'Column', alias: Optional[str] = None) -> 'SelectColumn':
        """
        Take a Column object and create a SelectColumn with the given alias.

        Args:
            column: The Column object to be converted.
            alias: An alternative name for the select statement, it will be used with the AS keyword
                and as a key by Cursor.

        Returns:
            SelectColumn: A SelectColumn object.
        """
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
        """
        A class that holds information about a table.

        Args:
            connection: A FileDB object connected to the database the table belongs to.
            name: The name of the table.
            columns: The columns of the table.
        """
        self.connection: 'FileDB' = connection
        self.name: str = name
        self.columns: list[Column] = columns

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def __len__(self) -> int:
        return self.connection.execute(f"select count(*) from {self.name}").fetchone()[0]

    def __iter__(self) -> Generator[dict[str, Any], None, None]:
        return self.select().fetchall()

    @property
    def keys(self) -> list[Column]:
        """
        The list of PRIMARY KEY columns in the table.

        Returns:
            A list of Column objects whose `primary_key` field is set to True.
        """
        return [c for c in self.columns if c.primary_key]

    def create_statement(self, exist_ok: bool = True) -> str:
        """
        Generate the expression that creates the table.

        Args:
            exist_ok: True if existing tables with the same name should be ignored.

        Returns:
            A CREATE TABLE expression.
        """
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
        """
        Select entries from the table.

        Args:
            columns: A list of columns to be selected, defaults to all the existing columns in the table.
            where: A WHERE expression.
            order_by: A list tuples containing one column (either as Column or string)
                and a sorting direction ("ASC", or "DESC").
            limit: The number of rows to limit the results to.
            parameters: Values to substitute in the SELECT expression, both in the `where` and SelectColumn statements.

        Returns:
            Cursor: A Cursor object wrapping the SQLite cursor returned by the SELECT transaction.
        """
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
        """
        Insert a row in the table. Existing rows with matching keys can be ignored or replaced.

        Args:
            entry: The row to be inserted as a dict with keys matching the names of the columns.
                The values need not be converted beforehand.
            exist_ok: True if existing rows with the same keys should be ignored, False otherwise
            replace: True if existing rows with the same keys should be replaced, False otherwise.
        """
        values: list[V] = [c.to_entry(entry[c.name]) for c in self.columns]

        elements: list[str] = ["INSERT"]

        if replace:
            elements.append("OR REPLACE")
        elif exist_ok:
            elements.append("OR IGNORE")

        elements.append(f"INTO {self.name}")

        elements.append(f"({','.join(c.name for c in self.columns)})")
        elements.append(f"VALUES ({','.join('?' * len(values))})")

        self.connection.execute(" ".join(elements), values)


class ModelTable(Table, Generic[M]):
    def __init__(self, connection: 'FileDB', name: str, model: Type[M]):
        """
        A class that holds information about a table using a model.

        Args:
            connection: A FileDB object connected to the database the table belongs to.
            name: The name of the table.
            model: The model representing the table.
        """
        super().__init__(connection, name, model_to_columns(model))
        self.model: Type[M] = model

    def __iter__(self) -> Generator[M, None, None]:
        return self.select().fetchall()

    def select(self, model: Type[M] = None,
               where: Optional[str] = None,
               order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
               limit: Optional[int] = None,
               parameters: Optional[list[Any]] = None) -> ModelCursor[M]:
        """
        Select entries from the table.

        Args:
            model: A model with the fields to be selected, defaults to the table's model.
            where: A WHERE expression.
            order_by: A list tuples containing one column (either as Column or string)
                and a sorting direction ("ASC", or "DESC").
            limit: The number of rows to limit the results to.
            parameters: Values to substitute in the SELECT expression, both in the `where` and SelectColumn statements.

        Returns:
            Cursor: A Cursor object wrapping the SQLite cursor returned by the SELECT transaction.
        """
        return ModelCursor[M](
            super().select(model_to_columns(model or self.model), where, order_by, limit, parameters).cursor,
            model or self.model, self
        )

    def insert(self, entry: M, exist_ok: bool = False, replace: bool = False):
        """
        Insert a row in the table. Existing rows with matching keys can be ignored or replaced.

        Args:
            entry: The row to be inserted as a model object with attributes matching the names of the columns.
            exist_ok: True if existing rows with the same keys should be ignored, False otherwise
            replace: True if existing rows with the same keys should be replaced, False otherwise.
        """
        super().insert(entry.model_dump(), exist_ok, replace)


# noinspection SqlNoDataSourceInspection
class View(Table):
    def __init__(self, connection: 'FileDB', name: str, on: Union[Table, str],
                 columns: list[Union[Column, SelectColumn]], where: Optional[str] = None,
                 group_by: Optional[list[Union[Column, SelectColumn]]] = None,
                 order_by: Optional[list[tuple[Union[str, Column], str]]] = None, limit: Optional[int] = None):
        """
        A subclass of Table to handle views.

        Args:
            connection: A FileDB object connected to the database the view belongs to.
            name: The name of the table.
            on: The table the view is based on.
            columns: The columns of the view.
            where: A WHERE expression for the view.
            group_by: A GROUP BY expression for the view.
            order_by: A list tuples containing one column (either as Column or string)
                and a sorting direction ("ASC", or "DESC").
            limit: The number of rows to limit the results to.
        """
        assert columns, "Views must have columns"
        super().__init__(connection, name, columns)
        self.on: Union[Table, str] = on
        self.where: str = where
        self.group_by: list[Union[Column, SelectColumn]] = group_by or []
        self.order_by: Optional[list[tuple[Union[str, Column], str]]] = order_by or []
        self.limit: Optional[int] = limit

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name}, on={self.on!r})"

    def create_statement(self, exist_ok: bool = True) -> str:
        """
        Generate the expression that creates the view.

        Args:
            exist_ok: True if existing views with the same name should be ignored.

        Returns:
            A CREATE VIEW expression.
        """
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
        """
        Select entries from the view.

        Args:
            columns: A list of columns to be selected, defaults to all the existing columns in the view.
            where: A WHERE expression.
            order_by: A list tuples containing one column (either as Column or string)
                and a sorting direction ("ASC", or "DESC").
            limit: The number of rows to limit the results to.
            parameters: Values to substitute in the SELECT expression, both in the `where` and SelectColumn statements.

        Returns:
            Cursor: A Cursor object wrapping the SQLite cursor returned by the SELECT transaction.
        """
        columns = columns or [
            Column(c.alias or c.name, c.sql_type, c.to_entry, c.from_entry, c.unique, c.primary_key, c.not_null,
                   c.check, c.default)
            for c in map(SelectColumn.from_column, self.columns)
        ]
        return super().select(columns, where, order_by, limit, parameters)

    def insert(self, *_args, **_kwargs):
        """
        Raises:
            OperationalError: Insert transactions are not allowed on views.
        """
        raise OperationalError("Cannot insert into view")


class ModelView(View, Generic[M]):
    def __init__(self, connection: 'FileDB', name: str, on: Union[Table, str], model: Type[M],
                 columns: list[Union[Column, SelectColumn]] = None, where: Optional[str] = None,
                 group_by: Optional[list[Union[Column, SelectColumn]]] = None,
                 order_by: Optional[list[tuple[Union[str, Column], str]]] = None, limit: Optional[int] = None):
        """
       A subclass of Table to handle views with models.

       Args:
           connection: A FileDB object connected to the database the view belongs to.
           name: The name of the table.
           on: The table the view is based on.
           model: A BaseModel subclass.
           columns: Optionally, the columns of the view if the model is too limited.
           where: A WHERE expression for the view.
           group_by: A GROUP BY expression for the view.
           order_by: A list tuples containing one column (either as Column or string)
               and a sorting direction ("ASC", or "DESC").
           limit: The number of rows to limit the results to.
       """
        super().__init__(connection, name, on, columns or model_to_columns(model), where, group_by, order_by, limit)
        self.model: Type[M] = model

    def select(self, model: Type[M] = None,
               where: Optional[str] = None,
               order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
               limit: Optional[int] = None,
               parameters: Optional[list[Any]] = None) -> ModelCursor[M]:
        return ModelCursor[M](
            super().select(model_to_columns(model or self.model), where, order_by, limit, parameters).cursor,
            model or self.model, self
        )


class FileDB(Connection):
    def __init__(self, database: str | bytes | PathLike[str] | PathLike[bytes], *,
                 timeout: float = 5.0,
                 detect_types: int = 0, isolation_level: Optional[str] = 'DEFERRED', check_same_thread: bool = True,
                 factory: Optional[Type[Connection]] = Connection, cached_statements: int = 100, uri: bool = False):
        """
        A wrapper class for an SQLite connection.

        Args:
            database: The path or URI to the database.
            timeout: How many seconds the connection should wait before raising an OperationalError
                when a table is locked.
            detect_types: Control whether and how data types not natively supported by SQLite are looked up to be
                converted to Python types.
            isolation_level: The isolation_level of the connection, controlling whether
                and how transactions are implicitly opened.
            check_same_thread: If True (default), ProgrammingError will be raised if the database connection
                is used by a thread other than the one that created it.
            factory: A custom subclass of Connection to create the connection with,
                if not the default Connection class.
            cached_statements: The number of statements that sqlite3 should internally cache for this connection,
                to avoid parsing overhead.
            uri: If set to True, database is interpreted as a URI with a file path and an optional query string.
        """
        from ..models.file import File, ConvertedFile
        from ..models.metadata import Metadata
        from ..models.identification import SignatureCount

        super().__init__(database, timeout, detect_types, isolation_level, check_same_thread, factory,
                         cached_statements, uri)

        self.files = self.create_table("Files", File)
        self.metadata = self.create_table("Metadata", Metadata)
        self.converted_files = self.create_table("_ConvertedFiles", ConvertedFile)

        self.not_converted = ModelView[File](self, "_NotConverted", self.files, self.files.model,
                                             f'"{self.files.name}".uuid IS NOT IN '
                                             f'(SELECT uuid from {self.converted_files.name})')
        self.identification_warnings = ModelView[File](self, "_IdentificationWarnings", self.files, self.files.model,
                                                       f'"{self.files.name}".warning IS NOT null')
        self.signature_count = ModelView[SignatureCount](
            self, "_SignatureCount",
            self.files,
            SignatureCount,
            [
                Column("puid", "varchar", or_none(str), or_none(str), False, False, False),
                Column("signature", "varchar", or_none(str), or_none(str), False, False, False),
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

    def __repr__(self):
        return f"{self.__class__.__name__}({self.path})"

    @property
    def path(self) -> Optional[Path]:
        for _, name, filename in self.execute("PRAGMA database_list"):
            if name == "main" and filename:
                return Path(filename)

        return None

    def init(self):
        """
        Create the Files and Metadata tables, and the _IdentificationWarnings and _SignatureCount views.
        """
        self.execute(self.files.create_statement(True))
        self.execute(self.metadata.create_statement(True))
        self.execute(self.identification_warnings.create_statement(True))
        self.execute(self.signature_count.create_statement(True))

    @overload
    def create_table(self, name: str, columns: Type[M]) -> ModelTable[M]:
        ...

    @overload
    def create_table(self, name: str, columns: list[Column]) -> Table:
        ...

    def create_table(self, name: str, columns: Union[Type[M], list[Column]]) -> Union[Table, ModelTable[M]]:
        if issubclass(columns, BaseModel):
            return ModelTable[M](self, name, columns)
        else:
            return Table(self, name, columns)
