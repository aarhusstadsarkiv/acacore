from datetime import datetime
from json import dumps
from json import loads
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor as SQLiteCursor
from sqlite3 import OperationalError
from typing import Any
from typing import Generator
from typing import Generic
from typing import Iterator
from typing import Optional
from typing import overload
from typing import Sequence
from typing import Type
from typing import TypeVar
from typing import Union

from pydantic.main import BaseModel

from acacore.utils.functions import or_none

from .column import Column
from .column import dump_object
from .column import model_to_columns
from .column import SelectColumn

T = TypeVar("T")
R = TypeVar("R")
M = TypeVar("M", bound=BaseModel)
V = Union[str, bytes, int, float, bool, datetime, None]


class Cursor:
    def __init__(
        self,
        cursor: SQLiteCursor,
        columns: list[Union[Column, SelectColumn]],
        table: Optional["Table"] = None,
    ) -> None:
        """
        A wrapper class for an SQLite cursor that returns its results as dicts (or objects).

        Args:
            cursor: An SQLite cursor from a select transaction.
            columns: A list of columns to use to convert the tuples returned by the cursor.
            table: Optionally, the Table from which on which the select transaction was executed.
        """
        self.cursor: SQLiteCursor = cursor
        self.columns: list[Union[Column, SelectColumn]] = columns
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
        return (tuple(c.from_entry(v) for c, v in zip(self.columns, vs)) for vs in self.cursor.fetchall())

    def fetchonetuple(self) -> Optional[tuple]:
        """
        Fetch one result from the cursor as tuples and convert the data using the given columns.

        Returns:
            tuple: A single tuple from the cursor.
        """
        vs: tuple = self.cursor.fetchone()

        return tuple(c.from_entry(v) for c, v in zip(self.columns, vs)) if vs else None

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
                model.model_validate(
                    {c.alias or c.name: c.from_entry(v) for c, v in zip(select_columns, vs)},
                )
                for vs in self.cursor.fetchall()
            )

        return (
            {c.alias or c.name: c.from_entry(v) for c, v in zip(select_columns, vs)} for vs in self.cursor.fetchall()
        )

    @overload
    def fetchone(self) -> Optional[dict[str, Any]]:
        ...

    @overload
    def fetchone(self, model: Type[M]) -> Optional[M]:
        ...

    def fetchone(
        self,
        model: Optional[Type[M]] = None,
    ) -> Optional[Union[dict[str, Any], M]]:
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

        entry: dict[str, Any] = {c.name: c.from_entry(v) for c, v in zip(select_columns, vs)}

        return model.model_validate(entry) if model else entry


class ModelCursor(Cursor, Generic[M]):
    def __init__(
        self,
        cursor: SQLiteCursor,
        model: Type[M],
        table: Optional["Table"] = None,
    ) -> None:
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


# noinspection SqlNoDataSourceInspection
class Table:
    def __init__(self, connection: "FileDBBase", name: str, columns: list[Column]) -> None:
        """
        A class that holds information about a table.

        Args:
            connection: A FileDBBase object connected to the database the table belongs to.
            name: The name of the table.
            columns: The columns of the table.
        """
        self.connection: "FileDBBase" = connection
        self.name: str = name
        self.columns: list[Column] = columns

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

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
            columns_elements: list[str] = []
            for column in self.columns:
                columns_elements.append(column.create_statement())
            if self.keys:
                columns_elements.append(f"primary key ({','.join(c.name for c in self.keys)})")
            elements.append(f"({','.join(columns_elements)})")

        return " ".join(elements)

    def create(self, exist_ok: bool = True):
        self.connection.execute(self.create_statement(exist_ok))

    def select(
        self,
        columns: Optional[list[Union[Column, SelectColumn]]] = None,
        where: Optional[str] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
        parameters: Optional[list[Any]] = None,
    ) -> Cursor:
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

        select_names = [f"{c.name} as {c.alias}" if c.alias else c.name for c in select_columns]

        statement: str = f"SELECT {','.join(select_names)} FROM {self.name}"

        if where:
            statement += f" WHERE {where}"

        if order_by:
            order_statements = [f"{c.name if isinstance(c, Column) else c} {s}" for c, s in order_by]
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
        values: list[V] = [c.to_entry(entry[c.name]) if c.name in entry else c.default_value() for c in self.columns]

        elements: list[str] = ["INSERT"]

        if replace:
            elements.append("OR REPLACE")
        elif exist_ok:
            elements.append("OR IGNORE")

        elements.append(f"INTO {self.name}")

        elements.append(f"({','.join(c.name for c in self.columns)})")
        elements.append(f"VALUES ({','.join('?' * len(values))})")

        self.connection.execute(" ".join(elements), values)

    def insert_many(
        self,
        entries: Union[Sequence[dict[str, Any]], Iterator[dict[str, Any]]],
        exist_ok: bool = False,
        replace: bool = False,
    ):
        """
        Insert multiple rows in the table. Existing rows with matching keys can be ignored or replaced.

        Args:
            entries: The rows to be inserted as a list (or iterator) of dicts with keys matching the names of
                the columns. The values need not be converted beforehand.
            exist_ok: True if existing rows with the same keys should be ignored, False otherwise
            replace: True if existing rows with the same keys should be replaced, False otherwise.
        """
        for entry in entries:
            self.insert(entry, exist_ok, replace)


class ModelTable(Table, Generic[M]):
    def __init__(self, connection: "FileDBBase", name: str, model: Type[M]) -> None:
        """
        A class that holds information about a table using a model.

        Args:
            connection: A FileDBBase object connected to the database the table belongs to.
            name: The name of the table.
            model: The model representing the table.
        """
        super().__init__(connection, name, model_to_columns(model))
        self.model: Type[M] = model

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[{self.model.__name__}]("{self.name}")'

    def __iter__(self) -> Generator[M, None, None]:
        return self.select().fetchall()

    def select(
        self,
        model: Optional[Type[M]] = None,
        where: Optional[str] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
        parameters: Optional[list[Any]] = None,
    ) -> ModelCursor[M]:
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
            super()
            .select(
                model_to_columns(model or self.model),
                where,
                order_by,
                limit,
                parameters,
            )
            .cursor,
            model or self.model,
            self,
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

    def insert_many(
        self,
        entries: Union[Sequence[M], Iterator[M]],
        exist_ok: bool = False,
        replace: bool = False,
    ):
        """
        Insert multiple rows in the table. Existing rows with matching keys can be ignored or replaced.

        Args:
            entries: The rows to be inserted as a list (or iterator) of model objects with attributes matching
                the names of the columns.
            exist_ok: True if existing rows with the same keys should be ignored, False otherwise
            replace: True if existing rows with the same keys should be replaced, False otherwise.
        """
        for entry in entries:
            self.insert(entry, exist_ok, replace)


# noinspection SqlResolve
class KeysTable:
    def __init__(self, connection: "FileDBBase", name: str, keys: list[Column]) -> None:
        """
        A class that holds information about a key-value pairs table.

        Args:
            connection: A FileDBBase object connected to the database the table belongs to.
            name: The name of the table.
            keys: The keys of the table.
        """
        self.keys: list[Column] = keys
        self.connection: "FileDBBase" = connection
        self.name: str = name
        self.columns: list[Column] = [
            Column("KEY", "text", str, str, True, True),
            Column("VALUE", "text", or_none(lambda o: dumps(dump_object(o))), or_none(loads)),
        ]

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

    def __len__(self) -> int:
        return len(self.keys)

    def __iter__(self) -> Generator[tuple[str, Any], None, None]:
        return ((k, v) for k, v in self.select().items())

    def create_statement(self, exist_ok: bool = True) -> str:
        """
        Generate the expression that creates the table.

        Args:
            exist_ok: True if existing tables with the same name should be ignored.

        Returns:
            A CREATE TABLE expression.
        """
        return Table(self.connection, self.name, self.columns).create_statement(exist_ok)

    def create(self, exist_ok: bool = True):
        self.connection.execute(self.create_statement(exist_ok))

    def select(self) -> Optional[dict[str, Any]]:
        """Return the data in the table as a dictionary."""
        data = dict(self.connection.execute(f"select KEY, VALUE from {self.name}").fetchall())
        return {c.name: c.from_entry(data[c.name]) for c in self.keys} if data else None

    def update(self, entry: dict[str, Any]):
        """
        Update the table with new data.

        Existing key-value pairs are replaced if the new entry contains an existing key.

        Args:
            entry: A dictionary with string keys.
        """
        entry = {k.lower(): v for k, v in entry.items()}
        entry = {c.name: c.to_entry(entry[c.name.lower()]) if c.name in entry else c.default_value() for c in self.keys}

        for key, value in entry.items():
            self.connection.execute(f"insert or replace into {self.name} (KEY, VALUE) values (?, ?)", [key, value])


class ModelKeysTable(KeysTable, Generic[M]):
    def __init__(self, connection: "FileDBBase", name: str, model: Type[M]) -> None:
        """
        A class that holds information about a key-value pairs table using a BaseModel for validation and parsing.

        Args:
            connection: A FileDBBase object connected to the database the table belongs to.
            name: The name of the table.
            model: The model of the table.
        """
        self.model: Type[M] = model
        super().__init__(connection, name, model_to_columns(model))

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[{self.model.__name__}]("{self.name}")'

    def select(self) -> Optional[M]:
        """Return the data in the table using the BaseModel object stored in the object."""
        data = super().select()
        return self.model.model_validate(data) if data else None

    def update(self, entry: M):
        """
        Update the table with new data.

        Existing key-value pairs are replaced if the new entry contains an existing key.

        Args:
            entry: A BaseModel object.
        """
        assert issubclass(type(entry), self.model), f"{type(entry).__name__} is not a subclass of {self.model.__name__}"
        super().update(entry.model_dump())


# noinspection SqlNoDataSourceInspection
class View(Table):
    def __init__(
        self,
        connection: "FileDBBase",
        name: str,
        on: Union[Table, str],
        columns: list[Union[Column, SelectColumn]],
        where: Optional[str] = None,
        group_by: Optional[list[Union[Column, SelectColumn]]] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
    ) -> None:
        """
        A subclass of Table to handle views.

        Args:
            connection: A FileDBBase object connected to the database the view belongs to.
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

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.name}", on={self.on!r})'

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
            f"{c.name} as {c.alias}" if c.alias else c.name for c in [SelectColumn.from_column(c) for c in self.columns]
        ]

        elements.append(
            f"SELECT {','.join(select_names)} " f"FROM {self.on.name if isinstance(self.on, Table) else self.on}",
        )

        if self.where:
            elements.append(f"WHERE {self.where}")

        if self.group_by:
            elements.append("GROUP BY")
            elements.append(
                ",".join(
                    [c.alias or c.name for c in [SelectColumn.from_column(c) for c in self.group_by]],
                ),
            )

        if self.order_by:
            order_statements = [
                f"{(SelectColumn.from_column(c).name or c.name) if isinstance(c, Column) else c} {s}"
                for c, s in self.order_by
            ]
            elements.append(f"ORDER BY {','.join(order_statements)}")

        if self.limit is not None:
            elements.append(f"LIMIT {self.limit}")

        return " ".join(elements)

    def select(
        self,
        columns: Optional[list[Union[Column, SelectColumn]]] = None,
        where: Optional[str] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
        parameters: Optional[list[Any]] = None,
    ) -> Cursor:
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
            Column(
                c.alias or c.name,
                c.sql_type,
                c.to_entry,
                c.from_entry,
                c.unique,
                c.primary_key,
                c.not_null,
                c.check,
                c.default,
            )
            for c in map(SelectColumn.from_column, self.columns)
        ]
        return super().select(columns, where, order_by, limit, parameters)

    def insert(self, *_args, **_kwargs):
        """
        Raises:
            OperationalError: Insert transactions are not allowed on views.
        """  # noqa: D205
        raise OperationalError("Cannot insert into view")

    def insert_many(self, *_args, **_kwargs):
        """
        Raises:
            OperationalError: Insert transactions are not allowed on views.
        """  # noqa: D205
        raise OperationalError("Cannot insert into view")


class ModelView(View, Generic[M]):
    def __init__(
        self,
        connection: "FileDBBase",
        name: str,
        on: Union[Table, str],
        model: Type[M],
        columns: Optional[list[Union[Column, SelectColumn]]] = None,
        where: Optional[str] = None,
        group_by: Optional[list[Union[Column, SelectColumn]]] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
    ) -> None:
        """
        A subclass of Table to handle views with models.

        Args:
            connection: A FileDBBase object connected to the database the view belongs to.
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
        super().__init__(
            connection,
            name,
            on,
            columns or model_to_columns(model),
            where,
            group_by,
            order_by,
            limit,
        )
        self.model: Type[M] = model

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[{self.model.__name__}]("{self.name}", on={self.on!r})'

    def select(
        self,
        model: Optional[Type[M]] = None,
        where: Optional[str] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
        parameters: Optional[list[Any]] = None,
    ) -> ModelCursor[M]:
        return ModelCursor[M](
            super()
            .select(
                model_to_columns(model or self.model),
                where,
                order_by,
                limit,
                parameters,
            )
            .cursor,
            model or self.model,
            self,
        )


class FileDBBase(Connection):
    def __init__(
        self,
        database: Union[str, bytes, PathLike[str], PathLike[bytes]],
        *,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: Optional[str] = "DEFERRED",
        check_same_thread: bool = True,
        factory: Optional[Type[Connection]] = Connection,
        cached_statements: int = 100,
        uri: bool = False,
    ) -> None:
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
        super().__init__(
            database,
            timeout,
            detect_types,
            isolation_level,
            check_same_thread,
            factory,
            cached_statements,
            uri,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.path})"

    @property
    def path(self) -> Optional[Path]:
        for _, name, filename in self.execute("PRAGMA database_list"):
            if name == "main" and filename:
                return Path(filename)

        return None

    @overload
    def create_table(self, name: str, columns: Type[M]) -> ModelTable[M]:
        ...

    @overload
    def create_table(self, name: str, columns: list[Column]) -> Table:
        ...

    def create_table(
        self,
        name: str,
        columns: Union[Type[M], list[Column]],
    ) -> Union[Table, ModelTable[M]]:
        """Create a table in the database.

        When the `columns` argument is a subclass of BadeModel, a ModelTable object is returned.

        Args:
            name: The name of the table.
            columns: A BaseModel subclass or the columns of the table.
        """
        if issubclass(columns, BaseModel):
            return ModelTable[M](self, name, columns)
        else:
            return Table(self, name, columns)

    @overload
    def create_keys_table(self, name: str, columns: Type[M]) -> ModelKeysTable[M]:
        ...

    @overload
    def create_keys_table(self, name: str, columns: list[Column]) -> KeysTable:
        ...

    def create_keys_table(
        self,
        name: str,
        columns: Union[Type[M], list[Column]],
    ) -> Union[KeysTable, ModelKeysTable[M]]:
        """
        Create a key-value pairs table in the database.

        When the `columns` argument is a subclass of BaseModel, a ModelTable object is returned.

        Args:
            name: The name of the table.
            columns: A BaseModel subclass or the columns of the table.
        """
        if issubclass(columns, BaseModel):
            return ModelKeysTable[M](self, name, columns)
        else:
            return KeysTable(self, name, columns)

    @overload
    def create_view(
        self,
        name: str,
        on: Union[Table, str],
        columns: Type[M],
        where: Optional[str] = None,
        group_by: Optional[list[Union[Column, SelectColumn]]] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
        *,
        select_columns: Optional[list[Union[Column, SelectColumn]]] = None,
    ) -> ModelView[M]:
        ...

    @overload
    def create_view(
        self,
        name: str,
        on: Union[Table, str],
        columns: list[Union[Column, SelectColumn]],
        where: Optional[str] = None,
        group_by: Optional[list[Union[Column, SelectColumn]]] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
    ) -> View:
        ...

    def create_view(
        self,
        name: str,
        on: Union[Table, str],
        columns: Union[list[Union[Column, SelectColumn]], Type[M]],
        where: Optional[str] = None,
        group_by: Optional[list[Union[Column, SelectColumn]]] = None,
        order_by: Optional[list[tuple[Union[str, Column], str]]] = None,
        limit: Optional[int] = None,
        *,
        select_columns: Optional[list[Union[Column, SelectColumn]]] = None,
    ) -> Union[View, ModelView[M]]:
        """Create a view in the database.

        When the `columns` argument is a subclass of BadeModel, a ModelView object is returned.

        Args:
            name: The name of the table.
            on: The table the view is based on.
            columns: A BaseModel subclass or the columns of the view.
            where: A WHERE expression for the view.
            group_by: A GROUP BY expression for the view.
            order_by: A list tuples containing one column (either as Column or string)
                and a sorting direction ("ASC", or "DESC").
            limit: The number of rows to limit the results to.
            select_columns: Optionally, the columns of the view if a model is given and is too limited.
        """
        if issubclass(columns, BaseModel):
            return ModelView[M](
                self,
                name,
                on,
                columns,
                select_columns,
                where,
                group_by,
                order_by,
                limit,
            )
        else:
            return View(self, name, on, columns, where, group_by, order_by, limit)
