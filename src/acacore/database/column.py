from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Self, Type
from uuid import UUID

from orjson import dumps, loads
from pydantic import BaseModel

from acacore.utils.functions import or_none

SQLValue = str | bytes | int | float | bool | datetime | None

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
    tuple[Callable[[Any | None], SQLValue], Callable[[SQLValue], Any | None]],
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


def _value_to_sql(value: SQLValue) -> str:
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


def _dump_object(obj: list | tuple | dict | BaseModel) -> list | dict:
    if isinstance(obj, dict):
        return {k: _dump_object(v) for k, v in obj.items()}
    elif issubclass(type(obj), BaseModel):
        return obj.model_dump(mode="json")
    elif isinstance(obj, (list, tuple)):
        return list(map(_dump_object, obj))
    else:
        return obj


@dataclass
class ColumnSpec:
    """
    Class representing a SQLite column.

    :ivar name: The name of the column.
    :ivar type: The SQLite type of the column.
    :ivar nullable: Whether the column is nullable.
    :ivar to_sql: A function that converts a value into an SQLIte-compatible type.
    :ivar from_sql: A function that converts an SQLIte return value into a different one..
    """

    name: str
    type: str
    to_sql: Callable[[Any | None], SQLValue]
    from_sql: Callable[[SQLValue], Any | None]
    nullable: bool

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r}, {self.type!r}, nullable={self.nullable})"

    def spec_sql(self) -> str:
        """
        The SQLite statement to create the column.

        :return: An SQLIte statement.
        """
        return (
            f"{self.name} {self.type} {'not null' if not self.nullable else ''}".strip()
        )

    @classmethod
    def from_schema(
        cls, name: str, schema: dict, defs: dict[str, dict] | None = None
    ) -> Self:
        """
        Generate a column from a JSON schema.

        :param name: The name of the column.
        :param schema: The JSON schema of the column.
        :param defs: The definitions in the schema.
        :return: A ``ColumnSpec`` instance.
        """
        defs = defs or {}
        if schema.get("$ref"):
            schema.update(defs[schema.get("$ref", "").removeprefix("#/$defs/")])
        schema_type: str | None = schema.get("type")
        schema_any_of: list[dict] = schema.get("anyOf", [])
        nullable: bool = any(s.get("type") == "null" for s in schema_any_of)

        sql_type: str
        to_sql: Callable[[Any | None], SQLValue]
        from_sql: Callable[[SQLValue], Any | None]

        if schema_type:
            sql_type = _sql_schema_types.get(schema_type)
            type_name: str = schema.get("format", schema_type)

            if schema_type in ("object", "array"):
                sql_type, to_sql, from_sql = (
                    "text",
                    lambda x: None
                    if x is None
                    else dumps(_dump_object(x), default=str).decode("utf-8"),
                    lambda x: None if x is None else loads(x),
                )
            elif type_name in _sql_schema_type_converters:
                to_sql, from_sql = _sql_schema_type_converters[type_name]
                to_sql, from_sql = or_none(to_sql), or_none(from_sql)
            else:
                raise TypeError(f"Cannot recognize type from schema {schema!r}")
        elif schema_any_of:
            if not schema_any_of[0] or len(schema_any_of) > 2:
                sql_type, to_sql, from_sql = (
                    "text",
                    lambda x: None
                    if x is None
                    else dumps(_dump_object(x), default=str).decode("utf-8"),
                    lambda x: None if x is None else loads(x),
                )
            else:
                spec = cls.from_schema(name, {**schema_any_of[0], **schema}, defs)
                sql_type, to_sql, from_sql = spec.type, spec.to_sql, spec.from_sql
        else:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")

        return cls(
            name=name,
            type=sql_type,
            to_sql=to_sql,
            from_sql=from_sql,
            nullable=nullable,
        )

    @classmethod
    def from_model(
        cls, model: Type[BaseModel], ignore: list[str] | None = None
    ) -> list[Self]:
        """
        Generate a list of columns from a Pydantic model.

        :param model: The model to create the columns from.
        :param ignore: A list of column names to ignore.
        :return: A list of ``ColumnSpec`` instances.
        """
        schema: dict = model.model_json_schema()
        ignore = ignore or []

        return [
            cls.from_schema(p, s, schema.get("$defs"))
            for p, s in schema["properties"].items()
            if p not in ignore
        ]
