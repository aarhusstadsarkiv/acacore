from os import PathLike
from pathlib import Path
from sqlite3 import DatabaseError

from packaging.version import Version
from pydantic import BaseModel

from acacore.models.event import Event
from acacore.models.file import ConvertedFile
from acacore.models.file import OriginalFile
from acacore.models.metadata import Metadata
from acacore.models.reference_files import TActionType

from .database import Database


class EventPath(Event):
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

        self.original_files = self.create_table(
            OriginalFile,
            "files_original",
            ["relative_path"],
            {"uuid": ["uuid"], "checksum": ["checksum"], "action": ["action"]},
            ["root"],
        )
        self.master_files = self.create_table(
            ConvertedFile,
            "files_master",
            ["relative_path"],
            {"uuid": ["uuid"], "checksum": ["checksum"], "action": ["action"]},
            ["root"],
        )
        self.access_files = self.create_table(
            ConvertedFile,
            "files_master",
            ["relative_path"],
            {"uuid": ["uuid"], "checksum": ["checksum"], "action": ["action"]},
            ["root"],
        )
        self.statutory_files = self.create_table(
            ConvertedFile,
            "files_statutory",
            ["relative_path"],
            {"uuid": ["uuid"], "checksum": ["checksum"], "action": ["action"]},
            ["root"],
        )

        self.log = self.create_table(
            Event,
            "log",
            indices={"uuid": ["uuid"], "time": ["time"], "operation": ["operation"]},
        )
        self.log_paths = self.create_view(
            EventPath,
            "log_paths",
            f"select f.relative_path as relative_path, h.* from {self.log.name} h left join {self.original_files.name} f on f.uuid = h.uuid",
        )

        self.identification_warnings = self.create_view(
            OriginalFile,
            "view_identification_warnings",
            f"select * from {self.original_files.name} where (warning is not null or puid is null) and size != 0",
        )

        self.signatures_count = self.create_view(
            SignatureCount,
            "view_signatures_count",
            f"select puid, count(*) as count from {self.original_files.name} group by puid, signature order by count desc",
        )

        self.actions_count = self.create_view(
            ActionCount,
            "view_actions_count",
            f"select action, count(*) as count from {self.original_files.name} group by action order by count desc",
        )

        self.checksums_count = self.create_view(
            ChecksumCount,
            "view_checksums_count",
            f"select checksum, count(*) as count from {self.original_files.name} group by checksum order by count desc",
        )

        self.metadata = self.create_keys_table(Metadata, "metadata", exist_ok=True)

    def is_initialised(self) -> bool:
        return self.metadata.name in self.tables()

    def version(self) -> Version:
        if self.is_initialised():
            return self.metadata.get("version")
        raise DatabaseError("Not initialised")
