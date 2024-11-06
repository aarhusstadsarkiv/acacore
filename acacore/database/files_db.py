from os import PathLike
from pathlib import Path
from sqlite3 import DatabaseError

from packaging.version import Version
from pydantic import BaseModel

from acacore.models.event import Event
from acacore.models.file import ConvertedFile
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.models.metadata import Metadata
from acacore.models.reference_files import TActionType

from .database import Database
from .database import KeysTable
from .database import Table
from .database import View


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
    def __init__(
        self,
        path: str | PathLike[str],
        *,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: str | None = "DEFERRED",
        check_same_thread: bool = True,
        cached_statements: int = 100,
    ) -> None:
        super().__init__(
            path,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
            cached_statements=cached_statements,
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
            {"uuid": ["uuid"], "checksum": ["checksum"], "original_uuid": ["original_uuid"]},
            ["root"],
        )
        self.access_files: Table[ConvertedFile] = Table(
            self.connection,
            ConvertedFile,
            "files_access",
            ["relative_path"],
            {"uuid": ["uuid"], "checksum": ["checksum"], "original_uuid": ["original_uuid"]},
            ["root"],
        )
        self.statutory_files: Table[ConvertedFile] = Table(
            self.connection,
            ConvertedFile,
            "files_statutory",
            ["relative_path"],
            {"uuid": ["uuid"], "checksum": ["checksum"], "original_uuid": ["original_uuid"]},
            ["root"],
        )

        self.log: Table[Event] = Table(
            self.connection,
            Event,
            "log",
            indices={"uuid": ["file_uuid", "file_type"], "time": ["time"], "operation": ["operation"]},
        )
        self.log_paths: View[EventPath] = View(
            self.connection,
            EventPath,
            "log_paths",
            f"""
            select coalesce(fo.relative_path, fm.relative_path) as file_relative_path, l.* from {self.log.name} l
                left join {self.original_files.name} fo on l.file_type = 'original' and fo.uuid = l.file_uuid
                left join {self.master_files.name} fm on l.file_type = 'master' and fm.uuid = l.file_uuid
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

    def upgrade(self):
        pass

    def is_initialised(self) -> bool:
        return self.metadata.name in self.tables()

    def version(self) -> Version:
        if self.is_initialised():
            return Version(self.metadata.get("version"))
        raise DatabaseError("Not initialised")

    def init(self):
        self.original_files.create(exist_ok=True)
        self.master_files.create(exist_ok=True)
        self.access_files.create(exist_ok=True)
        self.statutory_files.create(exist_ok=True)
        self.log.create(exist_ok=True)
        self.log_paths.create(exist_ok=True)
        self.identification_warnings.create(exist_ok=True)
        self.signatures_count.create(exist_ok=True)
        self.actions_count.create(exist_ok=True)
        self.checksums_count.create(exist_ok=True)
        self.metadata.create(exist_ok=True)
        if not self.metadata.get():
            self.metadata.set(Metadata())
