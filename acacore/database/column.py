from datetime import datetime
from json import dumps
from json import loads
from pathlib import Path
from typing import Callable
from typing import Generic
from typing import Optional
from typing import Sequence
from typing import Type
from typing import TypeVar
from typing import Union
from uuid import UUID

from pydantic import BaseModel

T = TypeVar("T")
V = Union[str, bytes, int, float, bool, datetime, None]

_sql_schema_types: dict[str, str] = {
    "string": "text",
    "integer": "integer",
    "number": "real",
    "boolean": "boolean",
    "bytes": "blob",
    "null": "text",
}

_sql_schema_type_converters: dict[
    str,
    tuple[Callable[[Optional[T]], V], Callable[[V], Optional[T]]],
] = {
    "path": (str, Path),
    "date-time": (datetime.isoformat, datetime.fromisoformat),
    "uuid4": (str, UUID),
    "binary": (bytes, bytes),
    "string": (str, str),
    "integer": (int, int),
    "number": (float, float),
    "boolean": (bool, bool),
    "null": (lambda x: x, lambda x: x),
}


def _value_to_sql(value: V) -> str:
    if value is None:
        return "null"
    elif isinstance(value, str):
        return value.replace("'", "\\'")
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, bytes):
        return f"X'{value.hex()}'"
    else:
        return str(value)


def dump_object(obj: Union[list, tuple, dict, BaseModel]) -> Union[list, dict]:
    if isinstance(obj, dict):
        return obj
    elif issubclass(type(obj), BaseModel):
        return obj.model_dump(mode="json")
    elif isinstance(obj, (list, tuple)):
        return list(map(dump_object, obj))
    else:
        return obj


def _schema_to_column(name: str, schema: dict, defs: Optional[dict[str, dict]] = None) -> Optional["Column"]:
    if schema.get("ignore"):
        return None

    defs = defs or {}
    if schema.get("$ref"):
        schema.update(defs[schema.get("$ref", "").removeprefix("#/$defs/")])
    schema_type: Optional[str] = schema.get("type", None)
    schema_any_of: list[dict] = schema.get("anyOf", [])

    sql_type: str
    to_entry: Callable[[Optional[T]], V]
    from_entry: Callable[[V], Optional[T]]
    not_null: bool = (schema_any_of or [{}])[-1].get("type", None) != "null"

    if schema_type:
        sql_type = _sql_schema_types.get(schema_type, None)
        type_name: str = schema.get("format", schema_type)

        if schema.get("enum") is not None:
            to_entry, from_entry = schema["enum"][0].__class__ if schema["enum"] else str, str
        elif schema_type in ("object", "array"):
            sql_type = "text"
            to_entry, from_entry = lambda o: dumps(dump_object(o), default=str), lambda o: loads(o)
        elif type_name in _sql_schema_type_converters:
            to_entry, from_entry = _sql_schema_type_converters[type_name]
        else:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")
    elif schema_any_of:
        if not schema_any_of[0] or len(schema_any_of) > 2:
            sql_type, to_entry, from_entry = "text", lambda o: dumps(dump_object(o), default=str), lambda x: loads(x)
        else:
            return _schema_to_column(name, {**schema_any_of[0], **schema}, defs)
    else:
        raise TypeError(f"Cannot recognize type from schema {schema!r}")

    return Column(
        name,
        sql_type,
        lambda x: None if x is None else to_entry(x),
        lambda x: None if x is None else from_entry(x),
        unique=schema.get("default", False),
        primary_key=schema.get("primary_key", False),
        not_null=not_null,
        default=schema.get("default", ...),
    )


def model_to_columns(model: Type[BaseModel]) -> list["Column"]:
    schema: dict = model.model_json_schema()
    columns = [_schema_to_column(p, s, schema.get("$defs")) for p, s in schema["properties"].items()]
    return [c for c in columns if c]


class Column(Generic[T]):
    def __init__(
        self,
        name: str,
        sql_type: str,
        to_entry: Callable[[T], V],
        from_entry: Callable[[V], T],
        unique: bool = False,
        primary_key: bool = False,
        not_null: bool = False,
        check: Optional[str] = None,
        default: Optional[T] = ...,
    ) -> None:
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

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{self.name}"
            f", {self.sql_type!r}"
            f", unique={self.unique}"
            f", primary_key={self.primary_key}"
            f", not_null={self.not_null}"
            f"{f', default={self.default!r}' if self.default is not Ellipsis else ''}"
            f")"
        )

    @classmethod
    def from_model(cls, model: Type[BaseModel]) -> list["Column"]:
        return model_to_columns(model)

    @property
    def check(self) -> str:
        return self._check.format(name=self.name) if self._check else ""

    @check.setter
    def check(self, check: Optional[str]):
        self._check = check

    def default_value(self) -> V:
        """
        Get the default value of the column formatted as an SQL parameter.

        Returns:
            An object of the return type of the column's to_entry function.

        Raises:
            ValueError: If the column does not have a set default value.
        """
        if self.default is Ellipsis:
            raise ValueError("Column does not have a default value")
        return self.to_entry(self.default)

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
            elements.append(f"default {_value_to_sql(self.default_value())}")
        if self.check:
            elements.append(f"check ({self.check})")

        return " ".join(elements)


class SelectColumn(Column):
    def __init__(
        self,
        name: str,
        from_entry: Callable[[V], T],
        alias: Optional[str] = None,
    ) -> None:
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
    def from_column(cls, column: Column, alias: Optional[str] = None) -> "SelectColumn":
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


class Index:
    def __init__(self, name: str, columns: Sequence[Column], unique: bool = False) -> None:
        """
        A class that stores information regarding an index.

        Args:
            name: The name of the index
            columns: The list of columns that the index applies to.
            unique: Whether the index is unique or not.
        """
        self.name: str = name
        self.columns: list[Column] = list(columns)
        self.unique: bool = unique

    def create_statement(self, table: str, exist_ok: bool = True):
        """
        Generate the expression that creates the index.

        Args:
            table: The name of the table.
            exist_ok: True if existing tables with the same name should be ignored.

        Returns:
            A CREATE TABLE expression.
        """
        return (
            f"create {'unique' if self.unique else ''} index {'if not exists' if exist_ok else ''} {self.name}"
            f" on {table} ({','.join(c.name for c in self.columns)})"
        )
