from datetime import datetime
from os import PathLike
from sqlite3 import Connection
from typing import Any, Optional, Type, Union
from uuid import UUID

from acacore.utils.functions import or_none

from .base import Column, FileDBBase, SelectColumn


class FileDB(FileDBBase):
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
        A class that handles the SQLite database used by AArhus City Archives to process data archives.

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
        from acacore.models.file import ConvertedFile, File
        from acacore.models.history import HistoryEntry
        from acacore.models.identification import SignatureCount
        from acacore.models.metadata import Metadata

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
        self.metadata = self.create_table("Metadata", Metadata)
        self.converted_files = self.create_table("_ConvertedFiles", ConvertedFile)
        self.history = self.create_table("History", HistoryEntry)

        self.not_converted = self.create_view(
            "_NotConverted",
            self.files,
            self.files.model,
            f'"{self.files.name}".uuid NOT IN ' f"(SELECT uuid from {self.converted_files.name})",
        )
        self.identification_warnings = self.create_view(
            "_IdentificationWarnings",
            self.files,
            self.files.model,
            f'"{self.files.name}".warning IS NOT null',
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

    def init(self):
        """Create the default tables and views."""
        self.files.create(True)
        self.metadata.create(True)
        self.converted_files.create(True)
        self.not_converted.create(True)
        self.identification_warnings.create(True)
        self.signature_count.create(True)
        self.commit()

    def is_empty(self) -> bool:
        return not self.files.select(limit=1).fetchone()

    def add_history(
        self,
        uuid: UUID,
        operation: str,
        data: Any,  # noqa: ANN401
        reason: Optional[str] = None,
        *,
        time: Optional[datetime] = None,
    ):
        self.history.insert(
            self.history.model(
                uuid=uuid,
                operation=operation,
                data=data,
                reason=reason,
                time=time or datetime.now(),  # noqa: DTZ005
            ),
        )
