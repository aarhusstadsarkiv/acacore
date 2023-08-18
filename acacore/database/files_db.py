"""File database backend."""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import re
from pathlib import Path
from typing import Any

import sqlalchemy as sql
from databases import Database
from pydantic import ValidationError, parse_obj_as
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import create_view

from ..exceptions.files import FileParseError
from ..models.file import ArchiveFile
from ..models.metadata import Metadata


# -----------------------------------------------------------------------------
# Database class
# -----------------------------------------------------------------------------


class FileDB(Database):
    """File database."""

    sql_meta = sql.MetaData()

    metadata = sql.Table(
        "Metadata",
        sql_meta,
        sql.Column("last_run", sql.DateTime, nullable=False),
        sql.Column("processed_dir", sql.String, nullable=False),
        sql.Column("file_count", sql.Integer),
        sql.Column("total_size", sql.Integer),
    )

    files = sql.Table(
        "Files",
        sql_meta,
        sql.Column("id", sql.Integer, primary_key=True, autoincrement=True),
        sql.Column("uuid", sql.String, nullable=False),
        sql.Column("relative_path", sql.String, nullable=False),
        sql.Column("checksum", sql.String),
        sql.Column("puid", sql.String),
        sql.Column("signature", sql.String),
        sql.Column("is_binary", sql.Boolean),
        sql.Column("file_size_in_bytes", sql.Integer),
        sql.Column("warning", sql.String),
    )

    multiple_files = sql.Table(
        "_MultipleFiles",
        sql_meta,
        sql.Column("path", sql.String, nullable=False),
    )

    empty_directories = sql.Table(
        "_EmptyDirectories",
        sql_meta,
        sql.Column("path", sql.String, nullable=False),
    )

    preservable_info = sql.Table(
        "Not_preservable",
        sql_meta,
        sql.Column("uuid", sql.String, primary_key=True, nullable=False),
        sql.Column("ignore reason", sql.String, nullable=False),
    )

    id_warnings = files.select().where(files.c.warning.isnot(None))
    puid_none = sql.case(
        [(files.c.puid.is_(None), "None")],
        else_=files.c.puid,
    )
    sig_count = (
        sql.select(
            [
                files.c.puid,
                files.c.signature,
                sql.func.count(puid_none).label("count"),
            ],
        )
        .group_by("puid")
        .order_by(sql.desc("count"))
    )
    create_view("_IdentificationWarnings", id_warnings, sql_meta)
    create_view("_SignatureCount", sig_count, sql_meta)

    def __init__(self, url: str) -> None:
        super().__init__(url)
        engine = sql.create_engine(url, connect_args={"check_same_thread": False})
        try:
            self.sql_meta.create_all(engine)
        except OperationalError as error:
            warn_re = re.compile(r"(?i)(IdentificationWarnings|SignatureCount)")
            if warn_re.search(str(error)):
                pass
            else:
                raise

    async def is_empty(self) -> bool:
        query = self.files.select()
        result = await self.fetch_one(query)
        return result is None

    async def delsert(self, table: sql.Table, values: Any) -> None:  # noqa: ANN401
        delete = table.delete()
        insert = table.insert()
        async with self.transaction():
            await self.execute(query=delete)
            if isinstance(values, list):
                await self.execute_many(query=insert, values=values)
            else:
                await self.execute(query=insert, values=values)

    async def set_metadata(self, metadata: Metadata) -> None:
        meta = {
            "processed_dir": str(metadata.processed_dir),
            **metadata.copy(exclude={"processed_dir"}).dict(),
        }
        await self.delsert(self.metadata, values=meta)

    async def set_files(self, files: list[ArchiveFile]) -> None:
        encoded_files = [file.encode() for file in files]
        await self.delsert(self.files, values=encoded_files)

    async def set_preservable_info(self, preservable_info: list[dict]) -> None:
        delete = self.preservable_info.delete()
        insert = self.preservable_info.insert()
        async with self.transaction():
            await self.execute(query=delete)
            await self.execute_many(query=insert, values=preservable_info)

    async def update_files(self, new_files: list[ArchiveFile]) -> None:
        encoded_files = [file.encode() for file in new_files]
        async with self.transaction():
            for file in encoded_files:
                update = self.files.update().where(self.files.c.uuid == file["uuid"]).values(file)
                await self.execute(update)

    async def get_files(self) -> list[ArchiveFile]:
        query = self.files.select()
        rows = await self.fetch_all(query)
        try:
            files: list[ArchiveFile] = parse_obj_as(list[ArchiveFile], rows)
        except ValidationError:
            raise FileParseError(
                """Failed to parse files as ArchiveFiles.
                    Rows from database: {}
                """.format(
                    rows,
                ),
            )
        return files

    async def set_multi_files(self, multi_files: list[Path]) -> None:
        await self.delsert(
            self.multiple_files,
            values=[{"path": str(path)} for path in multi_files],
        )

    async def set_empty_subs(self, empty_subs: list[Path]) -> None:
        await self.delsert(
            self.empty_directories,
            values=[{"path": str(path)} for path in empty_subs],
        )
