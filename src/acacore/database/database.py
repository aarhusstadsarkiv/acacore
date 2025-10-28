from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor as SQLiteCursor
from sqlite3 import OperationalError
from sqlite3 import ProgrammingError
from types import TracebackType
from typing import overload
from typing import Self

from psutil import AccessDenied
from psutil import Process
from psutil import process_iter
from pydantic import BaseModel

from .column import SQLValue
from .table import Table
from .table_keyvalue import KeysTable
from .table_view import View

_P = Sequence[SQLValue] | Mapping[str, SQLValue]


def _file_processes(path: Path) -> Generator[Process, None, None]:
    # noinspection PyUnresolvedReferences
    process_iter.cache_clear()
    for ps in process_iter():
        try:
            for f, *_ in ps.open_files():
                if Path(f).resolve() == path.resolve():
                    yield ps
                    continue
        except AccessDenied:
            continue
    yield from ()


class Database:
    """
    A class that handles an SQLite connection and allows accessing rows as Pydantic models.

    :ivar path: The path to the database file.
    :ivar readonly: Whether the connection is read-only.
    :ivar connection: The connection to thr SQLite database.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: str | None = "DEFERRED",
        check_same_thread: bool = True,
        cached_statements: int = 100,
        readonly: bool = False,
    ) -> None:
        """
        :param path: The path to the database.
        :param timeout: How many seconds the connection should wait before raising an OperationalError when a table
            is locked, defaults to 5.0.
        :param detect_types: Control whether and how data types not natively supported by SQLite are looked up to be
            converted to Python types, defaults to 0.
        :param isolation_level: The isolation_level of the connection, controlling whether and how transactions are
            implicitly opened, defaults to "DEFERRED".
        :param check_same_thread: If True (default), ProgrammingError will be raised if the database connection is
            used by a thread other than the one that created it, defaults to True.
        :param cached_statements: The number of statements that sqlite3 should internally cache for this connection,
            to avoid parsing overhead, defaults to 100.
        :param readonly: Whether to open the connection in read-only mode.
        """  # noqa: D205
        self.path: Path = Path(path).absolute()
        self.readonly: bool = readonly
        if check_same_thread and not self.readonly and (p := next(_file_processes(self.path), None)):
            raise OperationalError("Cannot open read-write connection to a database used by another process", p)
        self.connection: Connection = Connection(
            f"{self.path.as_uri()}?mode=ro" if self.readonly else self.path,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
            cached_statements=cached_statements,
            uri=True,
        )
        self._committed_changes: int = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException],
        _exc_val: BaseException,
        _exc_tb: TracebackType,
    ) -> None:
        self.close()

    @overload
    def execute(self, sql: str, /) -> SQLiteCursor: ...

    @overload
    def execute(self, sql: str, parameters: _P, /) -> SQLiteCursor: ...

    def execute(self, sql: str, parameters: _P | None = None, /) -> SQLiteCursor:
        """Executes an SQL statement."""
        return self.connection.execute(sql, parameters or [])

    def executemany(self, sql: str, parameters: Iterable[_P], /) -> SQLiteCursor:
        """Executes an SQL statement with multiple parameter lists."""
        return self.connection.executemany(sql, parameters)

    def commit(self):
        """Commit any pending transaction to the database."""
        self.connection.commit()
        self._committed_changes = self.total_changes

    def rollback(self):
        """Roll back to the start of any pending transaction."""
        self.connection.rollback()

    @property
    def total_changes(self):
        """Return the total number of database rows that have been modified, inserted, or deleted since the database connection was opened."""
        return self.connection.total_changes

    @property
    def committed_changes(self):
        """Return the total number of database row changes that have been committed since the database connection was opened."""
        return self._committed_changes

    @property
    def uncommitted_changes(self):
        """Return the total number of database row changes that have yet to be committed since the last transaction."""
        return self.total_changes - self._committed_changes

    def is_open(self) -> bool:
        """Return ``True`` if the database connection is open, else ``False``."""
        try:
            self.connection.execute("select 1 from sqlite_master limit 1")
            return True
        except ProgrammingError:
            return False

    def close(self):
        """Close the database connection."""
        self.connection.close()

    def tables(self) -> list[str]:
        """Return a list of table names in the database."""
        return [t for [t] in self.connection.execute("select name from sqlite_master where type = 'table'")]

    def views(self) -> list[str]:
        """Return a list of view names in the database."""
        return [v for [v] in self.connection.execute("select name from sqlite_master where type = 'view'")]

    def create_table[M: BaseModel](
        self,
        model: type[M],
        name: str,
        primary_keys: list[str] | None = None,
        indices: dict[str, list[str]] | None = None,
        ignore: list[str] | None = None,
        *,
        temporary: bool = False,
        exist_ok: bool = True,
    ) -> Table[M]:
        """
        Create a table in the database based on a model.

        :param model: The Pydantic model to create the table for.
        :param name: The name of the table.
        :param primary_keys: The primary keys of the table.
        :param indices: The indices of the table as index in the form {index name: list of indexed columns}.
        :param ignore: A list of field names to ignore from the model.
        :param temporary: Whether the table should be temporary (removed when connection closes) or not.
        :param exist_ok: Whether to ignore any existing table with the same name.
        :return: A ``Table`` instance.
        """
        return Table(self.connection, model, name, primary_keys, indices, ignore).create(
            temporary=temporary,
            exist_ok=exist_ok,
        )

    def create_view[M: BaseModel](
        self,
        model: type[M],
        name: str,
        select: str,
        ignore: list[str] | None = None,
        *,
        temporary: bool = False,
        exist_ok: bool = True,
    ) -> View[M]:
        """
        Create a view in the database based on a model.

        :param model: The Pydantic model to create the view for.
        :param name: The name of the view.
        :param select: The select SQL expression to use to populate the view.
        :param ignore: A list of field names to ignore from the model.
        :param temporary: Whether the view should be temporary (removed when connection closes) or not.
        :param exist_ok: Whether to ignore any existing view with the same name.
        :return: A ``View`` instance.
        """
        return View(self.connection, model, name, select, ignore).create(temporary=temporary, exist_ok=exist_ok)

    def create_keys_table[M: BaseModel](
        self,
        model: type[M],
        name: str,
        *,
        temporary: bool = False,
        exist_ok: bool = True,
    ) -> KeysTable[M]:
        """
        Create a key-value store table in the database based on a model.

        :param model: The Pydantic model to create the table for.
        :param name: The name of the table.
        :param temporary: Whether the table should be temporary (removed when connection closes) or not.
        :param exist_ok: Whether to ignore any existing table with the same name.
        :return: A ``KeysTable`` instance.
        """
        return KeysTable(self.connection, model, name).create(temporary=temporary, exist_ok=exist_ok)
