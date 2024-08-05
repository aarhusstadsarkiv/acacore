from datetime import datetime
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from typing import Type
from uuid import UUID

from pydantic import BaseModel

from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.metadata import Metadata
from acacore.models.reference_files import TActionType
from acacore.utils.functions import or_none

from .base import Column
from .base import FileDBBase
from .base import SelectColumn
from .column import model_to_columns
from .update import is_latest


class HistoryEntryPath(HistoryEntry):
    relative_path: Path | None = None


class SignatureCount(BaseModel):
    puid: str | None
    signature: str | None
    count: int | None


class ChecksumCount(BaseModel):
    checksum: str
    count: int


class ActionCount(BaseModel):
    action: TActionType
    count: int


class FileDB(FileDBBase):
    def __init__(
        self,
        database: str | bytes | PathLike[str] | PathLike[bytes],
        *,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: str | None = "DEFERRED",
        check_same_thread: bool = True,
        factory: Type[Connection] | None = Connection,
        cached_statements: int = 100,
        uri: bool = False,
        check_version: bool = False,
    ) -> None:
        """
        A class that handles the SQLite database used by AArhus City Archives to process data archives.

        :param database: The path or URI to the database.
        :param timeout: How many seconds the connection should wait before raising an OperationalError when a table
            is locked, defaults to 5.0.
        :param detect_types: Control whether and how data types not natively supported by SQLite are looked up to be
            converted to Python types, defaults to 0.
        :param isolation_level: The isolation_level of the connection, controlling whether and how transactions are
            implicitly opened, defaults to "DEFERRED".
        :param check_same_thread: If True (default), ProgrammingError will be raised if the database connection is
            used by a thread other than the one that created it, defaults to True.
        :param factory: A custom subclass of Connection to create the connection with, if not the default Connection
            class, defaults to Connection.
        :param cached_statements: The number of statements that sqlite3 should internally cache for this connection,
            to avoid parsing overhead, defaults to 100.
        :param uri: If set to True, database is interpreted as a URI with a file path and an optional query string,
            defaults to False.
        :param check_version: If set to True, check the database version and ensure it is the latest. Defaults to True.
        """
        super().__init__(
            database,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
            factory=factory,
            cached_statements=cached_statements,
            uri=uri,
        )

        self.files = self.create_table("Files", File)
        self.history = self.create_table("History", HistoryEntry)
        self.metadata = self.create_keys_table("Metadata", Metadata)

        self.history_paths = self.create_view(
            "_HistoryPaths",
            self.history,
            HistoryEntryPath,
            select_columns=[
                SelectColumn("F.relative_path", str, "relative_path"),
                *model_to_columns(HistoryEntry),
            ],
            joins=[f"left join {self.files.name} F on F.UUID = {self.history.name}.uuid"],
        )
        self.identification_warnings = self.create_view(
            "_IdentificationWarnings",
            self.files,
            self.files.model,
            f'"{self.files.name}".warning is not null or "{self.files.name}".puid is NULL',
        )
        self.checksum_count = self.create_view(
            "_ChecksumCount",
            self.files,
            ChecksumCount,
            None,
            [
                Column("checksum", "varchar", str, str, False, False, False),
            ],
            [
                (Column("count", "int", str, str), "DESC"),
            ],
            select_columns=[
                Column(
                    "checksum",
                    "varchar",
                    or_none(str),
                    or_none(str),
                    False,
                    False,
                    False,
                ),
                SelectColumn(
                    f'count("{self.files.name}.checksum")',
                    int,
                    "count",
                ),
            ],
        )
        self.signature_count = self.create_view(
            "_SignatureCount",
            self.files,
            SignatureCount,
            None,
            [
                Column("puid", "varchar", str, str, False, False, False),
            ],
            [
                (Column("count", "int", str, str), "ASC"),
            ],
            select_columns=[
                Column(
                    "puid",
                    "varchar",
                    or_none(str),
                    or_none(str),
                    False,
                    False,
                    False,
                ),
                Column(
                    "signature",
                    "varchar",
                    or_none(str),
                    or_none(str),
                    False,
                    False,
                    False,
                ),
                SelectColumn(
                    f"count("
                    f'CASE WHEN ("{self.files.name}".puid IS NULL) '
                    f"THEN 'None' "
                    f'ELSE "{self.files.name}".puid '
                    f"END)",
                    int,
                    "count",
                ),
            ],
        )
        self.actions_count = self.create_view(
            "_ActionsCount",
            self.files,
            ActionCount,
            None,
            [
                Column("action", "varchar", str, str, False, False, False),
            ],
            [
                (Column("count", "int", str, str), "DESC"),
            ],
            select_columns=[
                Column(
                    "action",
                    "varchar",
                    or_none(str),
                    or_none(str),
                    False,
                    False,
                    False,
                ),
                SelectColumn(
                    f'count("{self.files.name}.action")',
                    int,
                    "count",
                ),
            ],
        )

        if self.is_initialised():
            if check_version:
                is_latest(self, raise_on_difference=True)
        else:
            self.init()

    def is_initialised(self, *, check_views: bool = True, check_indices: bool = True) -> bool:
        tables: set[str] = {n.lower() for [n] in self.execute("select type, name from sqlite_master group by type")}
        if not {self.files.name.lower(), self.history.name.lower(), self.metadata.name.lower()}.issubset(set(tables)):
            return False

        if check_views:
            views: set[str] = {n.lower() for [n] in self.execute("select name from sqlite_master where type = 'view'")}
            expected_views: set[str] = {
                self.history_paths.name.lower(),
                self.identification_warnings.name.lower(),
                self.checksum_count.name.lower(),
                self.signature_count.name.lower(),
                self.actions_count.name.lower(),
            }
            if not expected_views.issubset(views):
                return False

        if check_indices:
            indices: set[str] = {
                n.lower() for [n] in self.execute("select name from sqlite_master where type = 'index'")
            }
            expected_indices: set[str] = {
                i.name.lower()
                for i in [
                    *self.history_paths.indices,
                    *self.identification_warnings.indices,
                    *self.checksum_count.indices,
                    *self.signature_count.indices,
                    *self.actions_count.indices,
                ]
            }
            if not expected_indices.issubset(indices):
                return False

        return True

    def init(self):
        self.files.create(True)
        self.history.create(True)
        self.metadata.create(True)
        self.history_paths.create(True)
        self.identification_warnings.create(True)
        self.checksum_count.create(True)
        self.signature_count.create(True)
        self.actions_count.create(True)
        self.metadata.update(self.metadata.model())
        self.commit()

    def is_empty(self) -> bool:
        return not self.files.select(limit=1).fetchone()

    def add_history(
        self,
        uuid: UUID | None,
        operation: str,
        data: dict | list | str | int | float | bool | datetime | None,
        reason: str | None = None,
        *,
        time: datetime | None = None,
    ) -> HistoryEntry:
        entry = self.history.model(
            uuid=uuid,
            operation=operation,
            data=data,
            reason=reason,
            time=time or datetime.now(),
        )
        self.history.insert(entry)
        return entry
