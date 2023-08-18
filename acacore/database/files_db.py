"""File database backend."""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import Any
from uuid import UUID

import sqlalchemy as sql
from databases import Database
from pydantic import ValidationError, parse_obj_as
# noinspection PyProtectedMember
from sqlalchemy.engine import Engine
# noinspection PyProtectedMember
from sqlalchemy.sql.expression import TextClause
from sqlalchemy_views import CreateView, DropView

from ..exceptions.files import FileParseError
from ..models.file import ArchiveFile
from ..models.metadata import Metadata


# -----------------------------------------------------------------------------
# Database class
# -----------------------------------------------------------------------------

def create_view(table: sql.Table, definition: TextClause, engine: Engine) -> None:
    view_drop = DropView(table, if_exists=True)
    view_create = CreateView(table, definition)
    engine.execute(view_drop)
    engine.execute(view_create)


# noinspection SqlNoDataSourceInspection
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

    converted_files = sql.Table(
        "_ConvertedFiles",
        sql_meta,
        sql.Column(
            "file_id",
            sql.Integer,
            sql.ForeignKey("Files.id"),
            nullable=False,
        ),
        sql.Column("uuid", sql.Unicode, nullable=False),
        extend_existing=True,
    )
    not_converted = sql.Table("_NotConverted", sql_meta)

    multiple_files = sql.Table(
        "_MultipleFiles",
        sql_meta,
        sql.Column("path", sql.String, nullable=False),
    )

    file_template_map = sql.Table(
        "ReplacedFiles",
        sql_meta,
        sql.Column("uuid", sql.Unicode, sql.ForeignKey("Files.uuid"), nullable=False),
        sql.Column(
            "Template_specifier",
            sql.Enum(
                "Damaged",
                "Empty",
                "Not_Convertable",
                "Not_Preservable",
                "Password_Protected",
            ),
            nullable=False,
        ),
        extend_existing=True,
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
        self.engine = sql.create_engine(url, connect_args={"check_same_thread": False})

        # Table reflection
        self.sql_meta.reflect(bind=self.engine)
        self.files = self.sql_meta.tables["Files"]
        self.converted_files.create(self.engine, checkfirst=True)
        self.file_template_map.create(self.engine, checkfirst=True)

        # Create _NotConverted view
        view_def = sql.text(
            "SELECT f.id, f.uuid, relative_path, puid, signature, warning "
            "FROM Files AS f "
            "LEFT JOIN _ConvertedFiles AS c "
            "ON f.uuid = c.uuid WHERE c.uuid IS NULL",
        )
        create_view(self.not_converted, view_def, self.engine)

    async def get_files(self) -> list[ArchiveFile]:
        query = self.files.select()
        rows = await self.fetch_all(query)
        try:
            files = parse_obj_as(list[ArchiveFile], rows)
        except ValidationError:
            raise FileParseError("Failed to parse files as ArchiveFiles.")
        else:
            return files

    async def get_pdf_files(self) -> list[ArchiveFile]:
        query = self.files.select().where(
            self.files.c.puid.in_(["fmt/18", "fmt/19", "fmt/20", "fmt/276"]),
        )
        rows = await self.fetch_all(query)
        try:
            files = parse_obj_as(list[ArchiveFile], rows)
        except ValidationError:
            # print(rows)
            raise FileParseError("Failed to parse files as ArchiveFiles.")
        else:
            return files

    async def get_non_conv_dbase_files(self) -> list[ArchiveFile]:
        query = (
            "SELECT * FROM Files WHERE Files.uuid NOT IN (SELECT uuid FROM _ConvertedFiles)"
            " AND Files.puid = 'x-fmt/9';"
        )
        rows = await self.fetch_all(query=query)
        try:
            files = parse_obj_as(list[ArchiveFile], rows)
        except ValidationError:
            # print(rows)
            raise FileParseError("Failed to parse files as ArchiveFiles.")
        else:
            return files

    async def get_not_converted_files(self) -> list[ArchiveFile]:
        # converted_files = await self.converted_uuids()
        # query = self.files.select().filter(files.c.uuid.notin_(list(converted_files)))
        # query = self.files.query.filter(files.uuid.notin_(list(converted_files)))

        query = "SELECT * FROM Files WHERE Files.uuid NOT IN (SELECT uuid FROM _ConvertedFiles);"
        rows = await self.fetch_all(query=query)
        try:
            files = parse_obj_as(list[ArchiveFile], rows)
        except ValidationError:
            # print(rows)
            raise FileParseError("Failed to parse files as ArchiveFiles.")
        else:
            return files

    async def update_status(self, uuid: UUID) -> None:
        async with self.transaction():
            get_file_id = self.files.select().where(self.files.c.uuid == str(uuid))
            file_id = await self.fetch_val(get_file_id, column="id")
            exists = await self.check_status(uuid)
            if not exists:
                insert_file = self.converted_files.insert()
                insert_values = {"file_id": file_id, "uuid": str(uuid)}
                await self.execute(insert_file, insert_values)
            else:
                return

    async def update_template_status(self, uuid: UUID, specifier: str) -> None:
        async with self.transaction():
            exists = await self.check_template_status(uuid)
            if not exists:
                insert_file = self.file_template_map.insert()
                insert_values = {
                    "uuid": str(uuid),
                    "Template_specifier": specifier,
                }
                await self.execute(insert_file, insert_values)
            else:
                return

    async def check_status(self, uuid: UUID) -> bool:
        conv_files = self.converted_files
        select_uuid = conv_files.select().where(conv_files.c.uuid == str(uuid))
        check = await self.fetch_one(select_uuid)
        return bool(check)

    async def check_template_status(self, uuid: UUID) -> bool:
        replaced_files = self.file_template_map
        select_uuid = replaced_files.select().where(replaced_files.c.uuid == str(uuid))
        check = await self.fetch_one(select_uuid)
        return bool(check)

    async def converted_uuids(self) -> set[UUID]:
        conv_files = self.converted_files
        query = conv_files.select()
        rows = await self.fetch_all(query)
        return {UUID(row["uuid"]) for row in rows}

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
