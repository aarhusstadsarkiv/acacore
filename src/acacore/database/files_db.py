from os import PathLike
from pathlib import Path
from sqlite3 import DatabaseError
from typing import Union

from packaging.version import Version
from pydantic import BaseModel

from acacore.models.event import Event
from acacore.models.file import AccessFile
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.models.file import StatutoryFile
from acacore.models.metadata import Metadata
from acacore.models.reference_files import TActionType

from .database import Database
from .database import KeysTable
from .database import Table
from .database import View
from .upgrade import is_latest
from .upgrade import upgrade
from .upgrade import UpgradeLogger


class EventPath(Event):
    file_relative_path: Path | None = None


class SignatureCount(BaseModel):
    puid: str | None
    signature: str | None
    count: int | None


class ChecksumCount(BaseModel):
    checksum: str
    count: int


class ActionCount(BaseModel):
    action: TActionType | None
    count: int


class FilesDB(Database):
    """
    A class that handles the SQLite database used by Aarhus City Archives to process data archives.

    :ivar original_files: The table containing the original files.
    :ivar master_files: The table containing the master archival files.
    :ivar access_files: The table containing the access files.
    :ivar statutory_files: The table containing the statutory files.
    :ivar log: The table containing the event log.
    :ivar log_paths: A view containing the event log together with the path of the files for events that reference them.
    :ivar identification_warnings: A view containing a list of files from "original files" that have identification issues.
    :ivar signatures_count: A view containing a list of all PUIDs from "original files" and how many times they occur.
    :ivar actions_count: A view containing a list of actions from "original files" and how many times they occur.
    :ivar checksums_count: A view containing a list of checksums from "original files" and how many times they occur.
    :ivar metadata: A table containing metadata about the database itself.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: str | None = "DEFERRED",
        check_same_thread: bool = True,
        check_initialisation: bool = False,
        check_version: bool = True,
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
        :param check_initialisation: If set to True, ensure the databse is initialized.
        :param check_version: If set to True, check the database version and ensure it is the latest.
        :param readonly: Whether to open the connection in read-only mode.
        """  # noqa: D205
        super().__init__(
            path,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
            cached_statements=cached_statements,
            readonly=readonly,
        )

        self.original_files: Table[OriginalFile] = Table(
            self.connection,
            OriginalFile,
            "files_original",
            ["relative_path"],
            {"uuid": ["uuid"], "checksum": ["checksum"], "action": ["action"]},
            ["root"],
        )
        self.master_files: Table[MasterFile] = Table(
            self.connection,
            MasterFile,
            "files_master",
            ["relative_path"],
            {
                "uuid": ["uuid"],
                "checksum": ["checksum"],
                "original_uuid": ["original_uuid"],
            },
            ["root"],
        )
        self.access_files: Table[AccessFile] = Table(
            self.connection,
            AccessFile,
            "files_access",
            ["relative_path"],
            {
                "uuid": ["uuid"],
                "checksum": ["checksum"],
                "original_uuid": ["original_uuid"],
            },
            ["root"],
        )
        self.statutory_files: Table[StatutoryFile] = Table(
            self.connection,
            StatutoryFile,
            "files_statutory",
            ["relative_path"],
            {
                "uuid": ["uuid"],
                "checksum": ["checksum"],
                "original_uuid": ["original_uuid"],
                "doc_id": ["doc_id"],
            },
            ["root"],
        )

        self.log: Table[Event] = Table(
            self.connection,
            Event,
            "log",
            indices={
                "uuid": ["file_uuid", "file_type"],
                "time": ["time"],
                "operation": ["operation"],
            },
        )
        self.log_paths: View[EventPath] = View(
            self.connection,
            EventPath,
            "log_paths",
            f"""
            select coalesce(fo.relative_path, fm.relative_path, fa.relative_path, fs.relative_path) as file_relative_path, l.* from {self.log.name} l
                left join {self.original_files.name}  fo on l.file_type = 'original'  and fo.uuid = l.file_uuid
                left join {self.master_files.name}    fm on l.file_type = 'master'    and fm.uuid = l.file_uuid
                left join {self.access_files.name}    fa on l.file_type = 'access'    and fa.uuid = l.file_uuid
                left join {self.statutory_files.name} fs on l.file_type = 'statutory' and fs.uuid = l.file_uuid
            """,
        )

        self.identification_warnings: View[OriginalFile] = View(
            self.connection,
            OriginalFile,
            "view_identification_warnings",
            f"select * from {self.original_files.name} where (warning is not null or puid is null) and size != 0",
            ignore=["root"],
        )

        self.signatures_count: View[SignatureCount] = View(
            self.connection,
            SignatureCount,
            "view_signatures_count",
            f"select puid, signature, count(*) as count from {self.original_files.name} group by puid, signature order by count desc",
        )

        self.actions_count: View[ActionCount] = View(
            self.connection,
            ActionCount,
            "view_actions_count",
            f"select action, count(*) as count from {self.original_files.name} group by action order by count desc",
        )

        self.checksums_count: View[ChecksumCount] = View(
            self.connection,
            ChecksumCount,
            "view_checksums_count",
            f"select checksum, count(*) as count from {self.original_files.name} group by checksum order by count desc",
        )

        self.metadata: KeysTable[Metadata] = KeysTable(self.connection, Metadata, "metadata")

        if check_initialisation and not self.is_initialised():
            raise DatabaseError("Database is not initialized")

        if check_version and self.is_initialised():
            is_latest(self.connection, raise_on_difference=True)

    def upgrade(self, files_root: str | PathLike[str], logger: UpgradeLogger | None = None):
        """
        Upgrade the database to the latest version.

        :raise DatabaseError: If the database is not initialized or if there are uncommitted changes.
        :param files_root: Root directory of the files.
        :param logger: A function called during upgrades to log events.
        """
        if not self.is_initialised():
            raise DatabaseError("Database is not initialized")
        if self.uncommitted_changes:
            raise DatabaseError("Database has uncommitted changes")
        upgrade(self.connection, files_root, logger)

    def is_initialised(self) -> bool:
        """
        Check if the database is initialised.

        :return: ``True`` if the database is initialised, ``False`` otherwise.
        """
        return self.metadata.name in self.tables() and self.metadata.get("version")

    def version(self) -> Version:
        """
        Get the database version.

        :return: The database version as a ``Version`` object.
        :raise DatabaseError: If the database is not initialized.
        """
        if self.is_initialised():
            return Version(self.metadata.get("version"))
        raise DatabaseError("Not initialised")

    # noinspection DuplicatedCode
    def init(self: Union[str, PathLike[str], "FilesDB"]) -> "FilesDB":
        """
        Initialize the database with all the necessary tables and views.

        :return: An instance of ``FilesDB``.
        """
        db = self if isinstance(self, FilesDB) else FilesDB(self)

        db.original_files.create(exist_ok=True)
        db.master_files.create(exist_ok=True)
        db.access_files.create(exist_ok=True)
        db.statutory_files.create(exist_ok=True)
        db.log.create(exist_ok=True)
        db.log_paths.create(exist_ok=True)
        db.identification_warnings.create(exist_ok=True)
        db.signatures_count.create(exist_ok=True)
        db.actions_count.create(exist_ok=True)
        db.checksums_count.create(exist_ok=True)
        db.metadata.create(exist_ok=True)
        if not db.metadata.get():
            db.metadata.set(Metadata())

        if not isinstance(self, FilesDB):
            db.commit()

        return db
