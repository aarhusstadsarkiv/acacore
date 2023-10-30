from pathlib import Path
from uuid import uuid4
from hashlib import sha256

from pytest import fixture

from acacore.database import FileDB
from acacore.database import model_to_columns
from acacore.database.base import ModelTable
from acacore.database.base import ModelView
from acacore.models.file import Action
from acacore.models.file import ConvertedFile
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.identification import SignatureCount
from acacore.models.metadata import Metadata
from random import randint


@fixture(scope="session")
def database_path(temp_folder: Path) -> Path:
    return temp_folder / "files.db"


@fixture(scope="session")
def test_file(test_files: Path, test_files_data: dict[str, dict]) -> File:
    filename, filedata = list(test_files_data.items())[0]
    file: Path = test_files / filename
    return File(
        id=randint(1, 10000),
        uuid=uuid4(),
        checksum=sha256(file.read_bytes()).hexdigest(),
        puid=filedata["matches"]["id"],
        relative_path=file.relative_to(test_files),
        is_binary=True,
        file_size_in_bytes=file.stat().st_size,
        signature=filedata["matches"]["format"],
        warning="; ".join(filedata["matches"]["warning"]),
        action=Action.CONVERT,
    )


def test_database_classes(database_path: Path):
    db: FileDB = FileDB(database_path)

    # Check tables classes
    assert isinstance(db.files, ModelTable) and issubclass(db.files.model, File)
    assert isinstance(db.metadata, ModelTable) and issubclass(db.metadata.model, Metadata)
    assert isinstance(db.converted_files, ModelTable) and issubclass(db.converted_files.model, ConvertedFile)
    assert isinstance(db.history, ModelTable) and issubclass(db.history.model, HistoryEntry)

    # Check views classes
    assert isinstance(db.not_converted, ModelView) and issubclass(db.not_converted.model, File)
    assert isinstance(db.identification_warnings, ModelView) and issubclass(db.identification_warnings.model, File)
    assert isinstance(db.signature_count, ModelView) and issubclass(db.signature_count.model, SignatureCount)


# noinspection SqlResolve,SqlNoDataSourceInspection
def test_database_tables(database_path: Path):
    database_path.unlink(missing_ok=True)

    db: FileDB = FileDB(database_path)

    # Create tables
    db.init()
    db.commit()

    # Test tables existence
    tables: list[str] = [
        t for [t] in db.execute("select name from sqlite_master where type = 'table' and name != 'sqlite_master'")
    ]
    assert db.files.name in tables
    assert db.metadata.name in tables
    assert db.converted_files.name in tables
    assert db.history.name in tables

    # Test views existence
    views: list[str] = [
        t for [t] in db.execute("select name from sqlite_master where type = 'view' and name != 'sqlite_master'")
    ]
    assert db.not_converted.name in views
    assert db.identification_warnings.name in views
    assert db.signature_count.name in views


def test_database_columns(database_path: Path):
    assert database_path.is_file()

    db: FileDB = FileDB(database_path)

    for table in (db.files, db.metadata, db.converted_files, db.history):
        columns_from_model = tuple(
            (
                column.name,
                column.sql_type.lower(),
                column.not_null,
                ("null" if column.default is None else column.to_entry(column.default))
                if column.default is not Ellipsis
                else None,
                column.primary_key,
            )
            for column in model_to_columns(table.model)
        )
        columns_from_sql = tuple(
            (column[1], column[2].lower(), bool(column[3]), column[4], bool(column[5]))
            for column in db.execute(f'pragma table_info("{table.name}")').fetchall()
        )
        assert columns_from_model == columns_from_sql


def test_insert_select(database_path: Path, test_file: File):
    assert database_path.is_file()

    db: FileDB = FileDB(database_path)
    test_file2 = test_file.model_copy(deep=True)
    test_file2.id = test_file.id // 2
    test_file2.uuid = uuid4()

    db.files.insert(test_file)
    db.files.insert(test_file2)
    db.commit()

    cursor = db.files.select(where="uuid = ?", parameters=[str(test_file.uuid)])
    result_file = cursor.fetchone()

    assert issubclass(cursor.model, File)
    assert cursor.table.name == db.files.name
    assert test_file.model_dump() == result_file.model_dump()

    cursor = db.files.select(order_by=[("ID", "asc")])
    result_files = list(cursor)
    assert len(result_files) == 2
    assert result_files[0].uuid == test_file2.uuid
    assert result_files[1].uuid == test_file.uuid

    cursor = db.files.select(order_by=[("ID", "desc")])
    result_files = list(cursor)
    assert len(result_files) == 2
    assert result_files[0].uuid == test_file.uuid
    assert result_files[1].uuid == test_file2.uuid