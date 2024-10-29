from pathlib import Path
from shutil import copy2
from sqlite3 import DatabaseError
from sqlite3 import IntegrityError
from sqlite3 import OperationalError
from uuid import uuid4

import pytest

from acacore.__version__ import __version__
from acacore.database import FileDB
from acacore.database import model_to_columns
from acacore.database.base import ModelTable
from acacore.database.base import ModelView

# noinspection PyProtectedMember
from acacore.database.column import _value_to_sql
from acacore.database.files_db import ActionCount
from acacore.database.files_db import ChecksumCount
from acacore.database.files_db import HistoryEntryPath
from acacore.database.files_db import SignatureCount
from acacore.database.upgrade import upgrade
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import Action
from acacore.models.reference_files import ConvertAction


@pytest.fixture
def database_path(temp_folder: Path) -> Path:
    path: Path = temp_folder / "files.db"
    path.unlink(missing_ok=True)
    with FileDB(path) as db:
        db.init()
    return path


@pytest.fixture(scope="session")
def test_databases(test_folder: Path, temp_folder: Path) -> list[Path]:
    files: list[Path] = [f for f in test_folder.joinpath("databases").iterdir() if f.is_file() and f.suffix == ".db"]
    files_copy: list[Path] = [temp_folder / f"test database {f.name}" for f in files]
    for src, dst in zip(files, files_copy):
        copy2(src, dst)
    return files_copy


@pytest.fixture(scope="session")
def test_file(test_files: Path, test_files_data: dict[str, dict]) -> File:
    filename, filedata = next(iter(test_files_data.items()))
    file_path: Path = test_files / filename
    action: Action = Action(
        name=filedata["matches"]["format"],
        action="convert",
        convert=ConvertAction(tool="convertool", output="odt"),
    )
    file: File = File.from_file(file_path)

    file.puid = filedata["matches"]["id"]
    file.signature = filedata["matches"]["format"]
    file.warning = filedata["matches"]["warning"]
    file.get_action({file.puid: action})

    return file


def test_database_connection(database_path: Path):
    db = FileDB(database_path)
    assert db.is_open

    db.close()
    assert not db.is_open

    with FileDB(database_path) as db:
        assert db.is_open
    assert not db.is_open


# noinspection DuplicatedCode
def test_database_classes(database_path: Path):
    db: FileDB = FileDB(database_path)

    # Check tables classes
    assert isinstance(db.files, ModelTable)
    assert issubclass(db.files.model, File)
    assert isinstance(db.history, ModelTable)
    assert issubclass(db.history.model, HistoryEntry)

    # Check views classes
    assert isinstance(db.history_paths, ModelView)
    assert issubclass(db.history_paths.model, HistoryEntryPath)
    assert isinstance(db.identification_warnings, ModelView)
    assert issubclass(db.identification_warnings.model, File)
    assert isinstance(db.checksum_count, ModelView)
    assert issubclass(db.checksum_count.model, ChecksumCount)
    assert isinstance(db.signature_count, ModelView)
    assert issubclass(db.signature_count.model, SignatureCount)
    assert isinstance(db.actions_count, ModelView)
    assert issubclass(db.actions_count.model, ActionCount)


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
    assert db.history.name in tables
    assert db.metadata.name in tables

    # Test views existence
    views: list[str] = [
        t for [t] in db.execute("select name from sqlite_master where type = 'view' and name != 'sqlite_master'")
    ]
    assert db.history_paths.name in views
    assert db.identification_warnings.name in views
    assert db.checksum_count.name in views
    assert db.signature_count.name in views
    assert db.actions_count.name in views


def test_database_columns(database_path: Path):
    assert database_path.is_file()

    db: FileDB = FileDB(database_path)

    for table in (db.files, db.history):
        columns_from_model = tuple(
            (
                column.name,
                column.sql_type.lower(),
                column.not_null,
                _value_to_sql(column.default_value()) if column.default is not Ellipsis else None,
                column.primary_key,
            )
            for column in model_to_columns(table.model)
        )
        columns_from_sql = tuple(
            (column[1], column[2].lower(), bool(column[3]), column[4], bool(column[5]))
            for column in db.execute(f'pragma table_info("{table.name}")').fetchall()
        )
        assert columns_from_model == columns_from_sql


def test_database_indices(database_path: Path):
    assert database_path.is_file()

    with FileDB(database_path) as db:
        for table in (db.files, db.history):
            for index in table.indices:
                assert (
                    db.execute(
                        "select 1 from sqlite_master where type = 'index' and tbl_name = ? and name = ?",
                        [table.name, index.name],
                    ).fetchone()
                    is not None
                )


def test_database_keys_tables(database_path: Path):
    assert database_path.is_file()

    db: FileDB = FileDB(database_path)

    metadata = db.metadata.select()

    assert isinstance(metadata, db.metadata.model)
    assert metadata.version == __version__

    metadata.version = __version__ + "-1"
    db.metadata.update(metadata)
    db.commit()

    metadata2 = db.metadata.select()

    assert metadata.model_dump() == metadata2.model_dump()


def test_insert_select(database_path: Path, test_file: File):
    assert database_path.is_file()

    db: FileDB = FileDB(database_path)
    test_file2 = test_file.model_copy(deep=True)
    test_file2.uuid = uuid4()

    db.files.insert(test_file)

    with pytest.raises(IntegrityError):
        db.files.insert(test_file2)

    test_file2.relative_path = test_file2.relative_path.with_suffix(".new")
    db.files.insert(test_file2)

    db.commit()

    cursor = db.files.select(where="uuid = ?", parameters=[str(test_file.uuid)])
    result_file = cursor.fetchone()

    assert issubclass(cursor.model, File)
    assert cursor.table.name == db.files.name
    assert test_file.model_dump() == result_file.model_dump()

    cursor = db.files.select(order_by=[("ROWID", "asc")])
    result_files = list(cursor)
    assert len(result_files) == 2
    assert result_files[0].uuid == test_file.uuid
    assert result_files[1].uuid == test_file2.uuid


def test_update(database_path: Path, test_file: File):
    assert database_path.is_file()

    test_file2 = test_file.model_copy(deep=True)
    test_file2.uuid = uuid4()

    db: FileDB = FileDB(database_path)
    db.files.insert(test_file, exist_ok=True)

    db.files.update(test_file2)
    db.commit()

    cursor = db.files.select(where="relative_path = ?", parameters=[str(test_file.relative_path)])
    result_file = cursor.fetchone()
    assert result_file is not None
    assert result_file.uuid == test_file2.uuid

    db.files.update({"uuid": test_file.uuid}, {"relative_path": test_file.relative_path})
    db.commit()

    cursor = db.files.select(where="relative_path = ?", parameters=[str(test_file.relative_path)])
    result_file = cursor.fetchone()
    assert result_file is not None
    assert result_file.uuid == test_file.uuid

    with pytest.raises(OperationalError):
        db.history.update({"uuid": test_file.uuid})

    with pytest.raises(KeyError):
        db.files.update({"uuid": test_file.uuid})


def test_history(database_path: Path):
    assert database_path.is_file()

    db: FileDB = FileDB(database_path)

    history = db.add_history(None, "DIGIARCH START", {"version": __version__})
    db.commit()

    history2 = db.history.select(where="TIME = ?", parameters=[history.time.isoformat()]).fetchone()

    assert history == history2


def test_database_upgrade(test_databases: list[Path]):
    for database_path in test_databases:
        with pytest.raises(DatabaseError):
            FileDB(database_path)

        with FileDB(database_path, check_version=False) as database:
            upgrade(database)
            assert database.metadata.select().version == __version__
