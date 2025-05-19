from pathlib import Path
from shutil import copy2
from sqlite3 import DatabaseError
from sqlite3 import OperationalError
from uuid import uuid4

import pytest
from packaging.version import Version

from acacore.__version__ import __version__
from acacore.database import FilesDB
from acacore.database.files_db import ActionCount
from acacore.database.files_db import ChecksumCount
from acacore.database.files_db import EventPath
from acacore.database.files_db import SignatureCount
from acacore.models.event import Event
from acacore.models.file import BaseFile
from acacore.models.file import ConvertedFile
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.models.file import StatutoryFile
from acacore.utils.functions import find_files


@pytest.fixture
def database_file(temp_folder: Path) -> Path:
    path: Path = temp_folder / "database.db"
    path.unlink(missing_ok=True)
    return path


def test_database_base(database_file: Path):
    with FilesDB(database_file) as db:
        assert db.is_open()
        assert not db.is_initialised()
        assert not db.tables()
        assert not db.views()
        assert db.total_changes == db.committed_changes == db.uncommitted_changes == 0
        with pytest.raises(DatabaseError, match="Not initialised"):
            db.version()
        db.init()
        assert db.version() == Version(__version__)
        assert db.total_changes == db.uncommitted_changes == 1
        assert db.committed_changes == 0
        db.commit()
        assert db.total_changes == db.committed_changes == 1
        assert db.uncommitted_changes == 0

    assert not db.is_open()


def test_database_temporary(database_file: Path):
    with FilesDB(database_file) as db:
        db.init()
        db.commit()

        view = db.create_view(OriginalFile, "_test_temp_view", "select * from files_original", temporary=True)
        assert view.select().fetchone() is None

        table = db.create_table(OriginalFile, "_test_temp_table", temporary=True)
        assert table.select().fetchone() is None

        table_keyvalue = db.create_keys_table(OriginalFile, "_test_temp_table_keyvalue", temporary=True)
        assert table_keyvalue.get() is None

    with FilesDB(database_file) as db:
        view.database = view._table.database = db.connection
        table.database = db.connection
        table_keyvalue.table.database = db.connection

        with pytest.raises(OperationalError, match=f"no such table: {view.name}"):
            view.select().fetchone()

        with pytest.raises(OperationalError, match=f"no such table: {table.name}"):
            table.select().fetchone()

        with pytest.raises(OperationalError, match=f"no such table: {table_keyvalue.name}"):
            table_keyvalue.get()


def test_database_tables(database_file: Path):
    with FilesDB(database_file) as db:
        db.init()
        db.commit()

        tables: list[str] = db.tables()
        views: list[str] = db.views()
        assert db.original_files.name in tables
        assert db.master_files.name in tables
        assert db.access_files.name in tables
        assert db.statutory_files.name in tables
        assert db.log.name in tables
        assert db.metadata.name in tables
        assert db.all_files.name in views
        assert db.log_paths.name in views
        assert db.identification_warnings.name in views
        assert db.signatures_count.name in views
        assert db.actions_count.name in views
        assert db.checksums_count.name in views


# noinspection DuplicatedCode
def test_database_insert_select(database_file: Path):
    with FilesDB(database_file) as db:
        db.init()
        db.commit()

        original_file = OriginalFile.from_file(database_file, database_file.parent)
        original_file2 = OriginalFile.from_file(__file__, Path(__file__).parent)
        db.original_files.insert(original_file)
        db.original_files[:] = original_file2
        db.master_files.insert(MasterFile.from_file(database_file, database_file.parent, original_file.uuid))
        db.access_files.insert(ConvertedFile.from_file(database_file, database_file.parent, original_file.uuid))
        db.statutory_files.insert(StatutoryFile.from_file(database_file, database_file.parent, original_file.uuid))
        db.log.insert(Event(file_uuid=original_file.uuid, file_type="original", operation="test_database_models"))
        db.commit()

        assert len(db.original_files) == 2
        assert len(db.master_files) == 1
        assert len(db.access_files) == 1
        assert len(db.statutory_files) == 1
        assert len(db.all_files) == (
            len(db.original_files) + len(db.master_files) + len(db.access_files) + len(db.statutory_files)
        )
        assert len(db.log) == 1
        assert len(db.log_paths) == 1
        assert len(db.identification_warnings) == 2
        assert len(db.signatures_count) == 1
        assert len(db.actions_count) == 1
        assert len(db.checksums_count) == 2

        inserted_file = db.original_files[{"uuid": str(original_file.uuid)}]
        assert isinstance(inserted_file, OriginalFile)
        assert inserted_file.root is None
        original_file.root = None
        assert inserted_file == original_file
        assert db.original_files[original_file] == inserted_file
        assert inserted_file in db.original_files

        assert isinstance(db.master_files.select().fetchone(), MasterFile)
        assert isinstance(db.access_files.select().fetchone(), ConvertedFile)
        assert isinstance(db.statutory_files.select().fetchone(), StatutoryFile)
        assert isinstance(db.all_files.select().fetchone(), BaseFile)
        assert isinstance(db.log.select().fetchone(), Event)

        assert isinstance(db.log_paths.select().fetchone(), EventPath)
        assert isinstance(db.identification_warnings.select().fetchone(), OriginalFile)
        assert isinstance(db.signatures_count.select().fetchone(), SignatureCount)
        assert isinstance(db.actions_count.select().fetchone(), ActionCount)
        assert isinstance(db.checksums_count.select().fetchone(), ChecksumCount)


def test_database_cursor(database_file: Path, test_folder: Path):
    with FilesDB(database_file) as db:
        db.init()
        db.commit()

        files: list[Path] = list(find_files(test_folder))
        db.original_files.insert(*(OriginalFile.from_file(f, test_folder) for f in files))
        db.commit()
        assert len(db.original_files) == len(files)

        cursor = db.original_files.select()
        assert cursor.fetchone() is not None
        assert next(cursor) is not None
        assert len(cursor.fetchmany(10)) == 10
        assert len(cursor.fetchall()) == len(files) - 1 - 1 - 10
        assert cursor.fetchone() is None
        with pytest.raises(StopIteration):
            next(cursor)


def test_database_update_delete(database_file: Path):
    with FilesDB(database_file) as db:
        db.init()
        db.commit()

        file1 = OriginalFile.from_file(database_file, database_file.parent)
        file2 = OriginalFile.from_file(database_file, database_file.parent)
        file1.root = file2.root = None

        db.original_files.insert(file1)
        db.commit()

        db.original_files[file1] = file2
        assert db.original_files[file1] == file2

        db.rollback()

        assert db.original_files.update(file2, file1) == 1
        assert db.original_files.update(file2, {"uuid": str(uuid4())}) == 0

        db.commit()

        assert len(db.original_files) == 1

        del db.original_files[file1]

        assert db.original_files[file1] is None
        assert len(db.original_files) == 0

        db.rollback()

        db.original_files.delete(file1)
        assert db.original_files[file1] is None
        assert len(db.original_files) == 0


def test_database_drop(database_file: Path):
    with FilesDB(database_file) as db:
        db.init()
        db.commit()

        view = db.create_view(OriginalFile, "_test_view", "select * from files_original")
        assert view.select().fetchone() is None
        view.drop()

        with pytest.raises(OperationalError, match=f"no such table: {view.name}"):
            view.select().fetchone()

        with pytest.raises(OperationalError, match=f"no such view: {view.name}"):
            view.drop(missing_ok=False)

        table = db.create_table(OriginalFile, "_test_table")
        assert table.select().fetchone() is None
        table.drop()

        with pytest.raises(OperationalError, match=f"no such table: {table.name}"):
            table.select().fetchone()

        with pytest.raises(OperationalError, match=f"no such table: {table.name}"):
            table.drop(missing_ok=False)

        table_keyvalue = db.create_keys_table(OriginalFile, "_test_table_keyvalue")
        assert table_keyvalue.get() is None
        table_keyvalue.drop()

        with pytest.raises(OperationalError, match=f"no such table: {table_keyvalue.name}"):
            table_keyvalue.get()

        with pytest.raises(OperationalError, match=f"no such table: {table_keyvalue.name}"):
            table_keyvalue.drop(missing_ok=False)


def test_database_upgrade(test_folder: Path, temp_folder: Path):
    database_file: Path = test_folder / "databases" / "v4_0_0.db"
    database_file_copy: Path = temp_folder / database_file.name
    database_file_copy.unlink(missing_ok=True)
    database_file_copy.parent.mkdir(parents=True, exist_ok=True)

    copy2(database_file, database_file_copy)

    with FilesDB(database_file_copy, check_version=False) as db:
        assert db.version() < Version(__version__)

        db.upgrade()

        assert db.version() == Version(__version__)

        assert db.original_files.select(limit=1).fetchone()
        assert db.master_files.select(limit=1).fetchone()

        for master_file in db.master_files:
            if master_file.processed == 0:
                assert db.access_files[{"original_uuid": str(master_file.uuid)}] is None
                assert db.statutory_files[{"original_uuid": str(master_file.uuid)}] is None
            elif master_file.processed == 1:
                assert db.access_files[{"original_uuid": str(master_file.uuid)}] is not None
                assert db.statutory_files[{"original_uuid": str(master_file.uuid)}] is None
            elif master_file.processed == 2:
                assert db.access_files[{"original_uuid": str(master_file.uuid)}] is None
                assert db.statutory_files[{"original_uuid": str(master_file.uuid)}] is not None
            elif master_file.processed == 3:
                assert db.access_files[{"original_uuid": str(master_file.uuid)}] is not None
                assert db.statutory_files[{"original_uuid": str(master_file.uuid)}] is not None
