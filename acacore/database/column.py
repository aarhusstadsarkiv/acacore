from datetime import datetime
from functools import reduce
from json import dumps
from json import loads
from pathlib import Path
from re import Pattern
from typing import Any
from typing import Callable
from typing import Generic
from typing import Literal
from typing import Optional
from typing import Sequence
from typing import Type
from typing import TypeVar
from uuid import UUID

from pydantic import AliasChoices
from pydantic import AliasPath
from pydantic import BaseModel
from pydantic import Discriminator
from pydantic import Field

# noinspection PyProtectedMember
from pydantic.config import JsonDict

# noinspection PyProtectedMember
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

SQLValue = str | bytes | int | float | bool | datetime | None
T = TypeVar("T")
V = TypeVar("V", str, bytes, int, float, bool, datetime, None)


# noinspection PyPep8Naming
def DBField(
    default: Any = PydanticUndefined,  # noqa: ANN401
    *,
    default_factory: Callable[[], Any] | None = PydanticUndefined,
    alias: str | None = PydanticUndefined,
    alias_priority: int | None = PydanticUndefined,
    validation_alias: str | AliasPath | AliasChoices | None = PydanticUndefined,
    serialization_alias: str | None = PydanticUndefined,
    title: str | None = PydanticUndefined,
    description: str | None = PydanticUndefined,
    examples: list[Any] | None = PydanticUndefined,
    exclude: bool | None = PydanticUndefined,
    discriminator: str | Discriminator | None = PydanticUndefined,
    deprecated: str | bool | None = PydanticUndefined,
    json_schema_extra: JsonDict | Callable[[JsonDict], None] | None = PydanticUndefined,
    frozen: bool | None = PydanticUndefined,
    validate_default: bool | None = PydanticUndefined,
    in_repr: bool = PydanticUndefined,
    init: bool | None = PydanticUndefined,
    init_var: bool | None = PydanticUndefined,
    kw_only: bool | None = PydanticUndefined,
    pattern: str | Pattern[str] | None = PydanticUndefined,
    strict: bool | None = PydanticUndefined,
    coerce_numbers_to_str: bool | None = PydanticUndefined,
    gt: float | None = PydanticUndefined,
    ge: float | None = PydanticUndefined,
    lt: float | None = PydanticUndefined,
    le: float | None = PydanticUndefined,
    multiple_of: float | None = PydanticUndefined,
    allow_inf_nan: bool | None = PydanticUndefined,
    max_digits: int | None = PydanticUndefined,
    decimal_places: int | None = PydanticUndefined,
    min_length: int | None = PydanticUndefined,
    max_length: int | None = PydanticUndefined,
    union_mode: Literal["smart", "left_to_right"] = PydanticUndefined,
    primary_key: bool | None = PydanticUndefined,
    index: list[str] | None = PydanticUndefined,
    ignore: bool | None = PydanticUndefined,
) -> FieldInfo:
    """
    A wrapper around ``pydantic.Field`` with added parameters for database specs.

    :param primary_key: Whether the field is a primary key.
    :param index: A list of indices the field belongs to.
    :param ignore: Whether the field should be ignored when creating the table spec.
    :param default: Default value if the field is not set.
    :param default_factory: A callable to generate the default value, such as ``~datetime.utcnow``.
    :param alias: The name to use for the attribute when validating or serializing by alias.
        This is often used for things like converting between snake and camel case.
    :param alias_priority: Priority of the alias. This affects whether an alias generator is used.
    :param validation_alias: Like ``alias``, but only affects validation, not serialization.
    :param serialization_alias: Like `alias`, but only affects serialization, not validation.
    :param title: Human-readable title.
    :param description: Human-readable description.
    :param examples: Example values for this field.
    :param exclude: Whether to exclude the field from the model serialization.
    :param discriminator: Field name or Discriminator for discriminating the type in a tagged union.
    :param deprecated: A deprecation message, an instance of ``warnings.deprecated`` or the
        ``typing_extensions.deprecated`` backport, or a boolean. If ``True``, a default deprecation message will be
        emitted when accessing the field.
    :param json_schema_extra: A dict or callable to provide extra JSON schema properties.
    :param frozen: Whether the field is frozen. If true, attempts to change the value on an instance will raise an
        error.
    :param validate_default: If ``True``, apply validation to the default value every time you create an instance.
        Otherwise, for performance reasons, the default value of the field is trusted and not validated.
    :param in_repr: A boolean indicating whether to include the field in the ``__repr__`` output.
    :param init: Whether the field should be included in the constructor of the dataclass.
        (Only applies to dataclasses.)
    :param init_var: Whether the field should _only_ be included in the constructor of the dataclass.
        (Only applies to dataclasses.)
    :param kw_only: Whether the field should be a keyword-only argument in the constructor of the dataclass.
        (Only applies to dataclasses.)
    :param coerce_numbers_to_str: Whether to enable coercion of any ``Number`` type to ``str`` (not applicable in
        ``strict`` mode).
    :param strict: If ``True``, strict validation is applied to the field.
    :param gt: Greater than. If set, value must be greater than this. Only applicable to numbers.
    :param ge: Greater than or equal. If set, value must be greater than or equal to this. Only applicable to numbers.
    :param lt: Less than. If set, value must be less than this. Only applicable to numbers.
    :param le: Less than or equal. If set, value must be less than or equal to this. Only applicable to numbers.
    :param multiple_of: Value must be a multiple of this. Only applicable to numbers.
    :param min_length: Minimum length for iterables.
    :param max_length: Maximum length for iterables.
    :param pattern: Pattern for strings (a regular expression).
    :param allow_inf_nan: Allow ``inf``, ``-inf``, ``nan``. Only applicable to numbers.
    :param max_digits: Maximum number of allow digits for strings.
    :param decimal_places: Maximum number of decimal places allowed for numbers.
    :param union_mode: The strategy to apply when validating a union. Can be ``smart`` (the default), or
        ``left_to_right``.
    :return: A FieldInfo object
    """
    extra: dict = (
        json_schema_extra
        if isinstance(json_schema_extra, dict)
        else json_schema_extra()
        if callable(json_schema_extra)
        else {}
    )
    if primary_key is not PydanticUndefined:
        extra["primary_key"] = primary_key
    if index is not PydanticUndefined:
        extra["index"] = index
    if ignore is not PydanticUndefined:
        extra["ignore"] = ignore

    return Field(
        default=default,
        default_factory=default_factory,
        alias=alias,
        alias_priority=alias_priority,
        validation_alias=validation_alias,
        serialization_alias=serialization_alias,
        title=title,
        description=description,
        examples=examples,
        exclude=exclude,
        discriminator=discriminator,
        deprecated=deprecated,
        frozen=frozen,
        validate_default=validate_default,
        repr=in_repr,
        init=init,
        init_var=init_var,
        kw_only=kw_only,
        pattern=pattern,
        strict=strict,
        coerce_numbers_to_str=coerce_numbers_to_str,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        allow_inf_nan=allow_inf_nan,
        max_digits=max_digits,
        decimal_places=decimal_places,
        min_length=min_length,
        max_length=max_length,
        union_mode=union_mode,
        json_schema_extra=extra,
    )


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


def dump_object(obj: list | tuple | dict | BaseModel) -> list | dict:
    if isinstance(obj, dict):
        return obj
    elif issubclass(type(obj), BaseModel):
        return obj.model_dump(mode="json")
    elif isinstance(obj, (list, tuple)):
        return list(map(dump_object, obj))
    else:
        return obj


def _schema_to_column(name: str, schema: dict, defs: dict[str, dict] | None = None) -> Optional["Column"]:
    if schema.get("ignore"):
        return None

    defs = defs or {}
    if schema.get("$ref"):
        schema.update(defs[schema.get("$ref", "").removeprefix("#/$defs/")])
    schema_type: str | None = schema.get("type")
    schema_any_of: list[dict] = schema.get("anyOf", [])

    sql_type: str
    to_entry: Callable[[T | None], V]
    from_entry: Callable[[V], T | None]
    not_null: bool = (schema_any_of or [{}])[-1].get("type", None) != "null"

    if schema_type:
        sql_type = _sql_schema_types.get(schema_type)
        type_name: str = schema.get("format", schema_type)

        if schema.get("enum") is not None:
            to_entry, from_entry = schema["enum"][0].__class__ if schema["enum"] else str, str
        elif schema_type in ("object", "array"):
            sql_type = "text"
            to_entry, from_entry = (
                lambda o: None if o is None else dumps(dump_object(o), default=str),
                lambda o: None if o is None else loads(o),
            )
        elif type_name in _sql_schema_type_converters:
            to_entry, from_entry = _sql_schema_type_converters[type_name]
        else:
            raise TypeError(f"Cannot recognize type from schema {schema!r}")
    elif schema_any_of:
        if not schema_any_of[0] or len(schema_any_of) > 2:
            sql_type, to_entry, from_entry = (
                "text",
                lambda x: None if x is None else dumps(dump_object(x), default=str),
                lambda x: None if x is None else loads(x),
            )
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


def model_to_indices(model: Type[BaseModel]) -> list["Index"]:
    columns: dict[str, Column] = {c.name: c for c in model_to_columns(model)}
    schema: dict = model.model_json_schema()
    indices: list[tuple[Column, str]] = [
        (columns[p], idx) for p, s in schema["properties"].items() if (idxs := s.get("index")) for idx in idxs
    ]
    unique_indices: list[tuple[Column, str]] = [
        (columns[p], idx) for p, s in schema["properties"].items() if (idxs := s.get("unique_index")) for idx in idxs
    ]
    indices_merged: dict[str, list[Column]] = reduce(lambda i, c: i | {c[1]: [*i.get(c[1], []), c[0]]}, indices, {})
    indices_merged |= reduce(lambda i, c: i | {c[1]: [*i.get(c[1], []), c[0]]}, unique_indices, {})
    return [Index(n, cs) for n, cs in indices_merged.items()]


class Column(Generic[T, V]):
    def __init__(
        self,
        name: str,
        sql_type: str,
        to_entry: Callable[[T], V],
        from_entry: Callable[[V], T],
        unique: bool = False,
        primary_key: bool = False,
        not_null: bool = False,
        check: str | None = None,
        default: T | None = ...,
    ) -> None:
        """
        A class that stores information regarding a table column.

        :param name: The name of the column.
        :param sql_type: The SQL type to use when creating a table.
        :param to_entry: A function that returns a type supported by SQLite
            (str, bytes, int, float, bool, datetime, or None).
        :param from_entry: A function that takes a type returned by SQLite (str, bytes, int, float, or None) and
            returns another object.
        :param unique: True if the column should be set as UNIQUE, defaults to False.
        :param primary_key: True if the column is a PRIMARY KEY, defaults to False.
        :param not_null: True if the column is NOT NULL, defaults to False.
        :param check: A string containing a CHECK expression, {name} substrings will be substituted with the name of
            the column, defaults to None.
        :param default: The column's DEFAULT value, which will be converted using `to_entry`.
            Note that None is considered a valid default value; to set it to empty use Ellipsis (...), defaults to ....
        """
        self.name: str = name
        self.sql_type: str = sql_type
        self.to_entry: Callable[[T], V] = to_entry
        self.from_entry: Callable[[V], T] = from_entry
        self.unique: bool = unique
        self.primary_key: bool = primary_key
        self.not_null: bool = not_null
        self._check: str = check or ""
        self.default: T | Ellipsis | None = default

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
    def check(self, check: str | None):
        self._check = check

    def default_value(self) -> V:
        """
        Get the default value of the column formatted as an SQL parameter.

        :raises ValueError: If the column does not have a set default value.
        :return: An object of the return type of the column's to_entry function.
        """
        if self.default is Ellipsis:
            raise ValueError("Column does not have a default value")
        return self.to_entry(self.default)

    def create_statement(self) -> str:
        """
        Generate the statement that creates the column.

        :return: A column statement for a CREATE TABLE expression.
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


class SelectColumn(Column, Generic[T, V]):
    def __init__(
        self,
        name: str,
        from_entry: Callable[[V], T],
        alias: str | None = None,
    ) -> None:
        """
        A subclass of Column for SELECT expressions that need complex statements and/or an alias.

        :param name: The name or select statement for the select expression (e.g., count(*)).
        :param from_entry: A function that takes a type returned by SQLite (str, bytes, int, float, or None) and
            returns another object.
        :param alias: An alternative name for the select statement, it will be used with the AS keyword and as a key
            by Cursor, defaults to None.
        """
        super().__init__(name, "", lambda x: x, from_entry)
        self.alias: str | None = alias

    @classmethod
    def from_column(cls, column: Column, alias: str | None = None) -> "SelectColumn":
        """
        Take a Column object and create a SelectColumn with the given alias.

        :param column: The Column object to be converted.
        :param alias: An alternative name for the select statement, it will be used with the AS keyword and as a key
            by Cursor, defaults to None.
        :return: A SelectColumn object.
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

        :param name: The name of the index.
        :param columns: The list of columns that the index applies to.
        :param unique: Whether the index is unique or not, defaults to False.
        """
        self.name: str = name
        self.columns: list[Column] = list(columns)
        self.unique: bool = unique

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{self.name}"
            f", unique={self.unique}"
            f", columns={[c.name for c in self.columns]}"
            f")"
        )

    def create_statement(self, table: str, exist_ok: bool = True):
        """
        Generate the expression that creates the index.

        :param table: The name of the table.
        :param exist_ok: True if existing tables with the same name should be ignored, defaults to True.
        :return: A CREATE TABLE expression.
        """
        return (
            f"create {'unique' if self.unique else ''} index {'if not exists' if exist_ok else ''} {self.name}"
            f" on {table} ({','.join(c.name for c in self.columns)})"
        )
