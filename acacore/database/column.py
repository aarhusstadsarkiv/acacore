from datetime import datetime
from pathlib import Path
from typing import Callable
from typing import Generic
from typing import Optional
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

_sql_schema_type_converters: dict[str, tuple[Callable[[Optional[T]], V], Callable[[V], Optional[T]]]] = {
    "path": (str, Path),
    "date-time": (datetime.isoformat, datetime.fromisoformat),
    "uuid4": (str, UUID),
    "binary": (bytes, bytes),
    "string": (str, str),
    "integer": (float, float),
    "number": (float, float),
    "boolean": (bool, bool),
    "null": (lambda x: x, lambda x: x),
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
        type_name: str = schema.get("format", schema_type)

        if type_name in _sql_schema_type_converters:
            to_entry, from_entry = _sql_schema_type_converters[type_name]
        else:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")
    elif schema_any_of:
        if schema_any_of[-1].get("type", None) != "null" and len(schema_any_of) > 1:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")
        elif len(schema_any_of) > 2:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")

        return _schema_to_column(name, {**schema_any_of[0], **schema})
    else:
        raise TypeError(f"Cannot recognize type from schema {schema!r}")

    return Column(
        name, sql_type, lambda x: None if x is None else to_entry(x), lambda x: None if x is None else from_entry(x),
        unique=schema.get("default", False),
        primary_key=schema.get("primary_key", False),
        not_null=not_null,
        default=schema.get("default", ...)
    )


def model_to_columns(model: Type[BaseModel]) -> list['Column']:
    return [_schema_to_column(p, s) for p, s in model.model_json_schema()["properties"].items()]


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
    def from_column(cls, column: Column, alias: Optional[str] = None) -> 'SelectColumn':
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
