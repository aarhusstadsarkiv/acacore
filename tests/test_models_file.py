from os import environ
from pathlib import Path
from random import random
from uuid import uuid4

import pytest

from acacore.models.file import BaseFile
from acacore.models.file import ConvertedFile
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.models.reference_files import Action
from acacore.models.reference_files import ConvertAction
from acacore.models.reference_files import CustomSignature
from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures
from acacore.siegfried import Siegfried


@pytest.fixture
def siegfried(siegfried_folder: Path) -> Siegfried:
    return Siegfried(Path(environ["GOPATH"], "bin", "sf"), "pronom.sig", siegfried_folder)


@pytest.fixture(scope="session")
def test_file(test_files: Path, test_files_data: dict[str, dict]) -> Path:
    files = sorted((n for n, d in test_files_data.items() if d["binary"] and d["matches"]), key=lambda _: random())
    return test_files / files[0]


@pytest.fixture(scope="session")
def actions() -> dict[str, Action]:
    return get_actions()


@pytest.fixture(scope="session")
def custom_signatures() -> list[CustomSignature]:
    return get_custom_signatures()


def test_base_file(
    test_folder: Path,
    test_file: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    custom_signatures: list[CustomSignature],
):
    uuid = uuid4()
    file = BaseFile.from_file(
        test_file,
        test_folder,
        siegfried,
        custom_signatures,
        uuid,
    )
    assert file.relative_path == test_file.relative_to(test_folder)
    assert file.root == test_folder
    assert file.uuid == uuid
    assert file.checksum == test_files_data[test_file.name]["checksum"]
    assert file.is_binary == test_files_data[test_file.name]["binary"]
    assert file.size == test_files_data[test_file.name]["filesize"]
    assert file.puid == test_files_data[test_file.name]["matches"]["id"]
    assert file.signature == test_files_data[test_file.name]["matches"]["format"]
    assert set(file.warning or []) == set(test_files_data[test_file.name]["matches"]["warning"])


def test_original_file(
    test_folder: Path,
    test_file: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    actions: dict[str, Action],
    custom_signatures: list[CustomSignature],
) -> None:
    uuid = uuid4()
    parent = uuid4()
    processed = False
    lock = True
    file = OriginalFile.from_file(
        test_file,
        test_folder,
        siegfried,
        actions,
        custom_signatures,
        uuid,
        parent,
        processed,
        lock,
    )
    assert file.puid in actions
    assert file.action == actions[file.puid].action
    assert file.action_data == actions[file.puid].action_data
    assert file.parent == parent
    assert file.processed == processed
    assert file.lock == lock
    assert file.original_name == test_file.name


def test_converted_file(
    test_folder: Path,
    test_file: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    custom_signatures: list[CustomSignature],
):
    uuid = uuid4()
    original_uuid = uuid4()
    file = ConvertedFile.from_file(
        test_file,
        test_folder,
        original_uuid,
        siegfried,
        custom_signatures,
        uuid,
    )
    assert file.original_uuid == original_uuid


def test_master_file(
    test_folder: Path,
    test_file: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    actions: dict[str, Action],
    custom_signatures: list[CustomSignature],
) -> None:
    convert_actions: dict[str, ConvertAction] = {p: a.convert for p, a in actions.items() if a.convert}
    uuid = uuid4()
    original_uuid = uuid4()
    file = MasterFile.from_file(
        test_file,
        test_folder,
        original_uuid,
        siegfried,
        custom_signatures,
        convert_actions,
        {"": next(iter(convert_actions.values()))},
        uuid,
    )
    assert file.convert_access == convert_actions.get(file.puid)
    assert file.convert_statutory is None
