from os import environ
from pathlib import Path
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
def actions() -> dict[str, Action]:
    return get_actions()


@pytest.fixture(scope="session")
def custom_signatures() -> list[CustomSignature]:
    return get_custom_signatures()


def test_base_file(
    test_folder: Path,
    test_files: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    custom_signatures: list[CustomSignature],
):
    for filename, filedata in test_files_data.items():
        filepath = test_files / filename
        uuid = uuid4()
        file = BaseFile.from_file(
            test_files / filename,
            test_folder,
            siegfried,
            custom_signatures,
            uuid,
        )
        assert file.relative_path == filepath.relative_to(test_folder)
        assert file.root == test_folder
        assert file.uuid == uuid
        assert file.checksum == test_files_data[filepath.name]["checksum"]
        assert file.is_binary == test_files_data[filepath.name]["binary"]
        assert file.size == test_files_data[filepath.name]["filesize"]
        assert file.puid == test_files_data[filepath.name]["matches"]["id"]
        if file.puid:
            assert file.signature == test_files_data[filepath.name]["matches"]["format"]
            assert set(file.warning or []) == set(test_files_data[filepath.name]["matches"]["warning"])
        else:
            assert file.signature is None
            assert file.warning is None


def test_original_file(
    test_folder: Path,
    test_files: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    actions: dict[str, Action],
    custom_signatures: list[CustomSignature],
) -> None:
    for filename, filedata in test_files_data.items():
        uuid = uuid4()
        parent = uuid4()
        processed = False
        lock = True
        file = OriginalFile.from_file(
            test_files / filename,
            test_folder,
            siegfried,
            actions,
            custom_signatures,
            uuid,
            parent,
            processed,
            lock,
        )
        assert file.parent == parent
        assert file.processed == processed
        assert file.lock == lock
        assert file.original_path == test_files.joinpath(filename).relative_to(test_folder)

        action = actions.get(filedata["matches"]["id"])

        if action and action.reidentify:
            assert file.puid in (filedata["matches"]["id"], None) or file.puid in [cs.puid for cs in custom_signatures]

        if file.puid and (action := actions.get(file.puid)):
            assert all(d == file.action_data.model_dump()[a] for a, d in action.action_data.model_dump().items() if d)
            assert file.action == action.action or (action.ignore_if and file.action == "ignore")


def test_converted_file(
    test_folder: Path,
    test_files: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    custom_signatures: list[CustomSignature],
):
    for filename in test_files_data.keys():
        uuid = uuid4()
        original_uuid = uuid4()
        file = ConvertedFile.from_file(
            test_files / filename,
            test_folder,
            original_uuid,
            siegfried,
            custom_signatures,
            uuid,
        )
        assert file.original_uuid == original_uuid


def test_master_file(
    test_folder: Path,
    test_files: Path,
    test_files_data: dict[str, dict],
    siegfried: Siegfried,
    actions: dict[str, Action],
    custom_signatures: list[CustomSignature],
) -> None:
    convert_actions: dict[str, ConvertAction] = {p: a.convert for p, a in actions.items() if a.convert}
    convert_actions_empty: dict[str, ConvertAction] = {"": next(iter(convert_actions.values()))}
    for filename, filedata in test_files_data.items():
        uuid = uuid4()
        original_uuid = uuid4()
        file = MasterFile.from_file(
            test_files / filename,
            test_folder,
            original_uuid,
            siegfried,
            custom_signatures,
            convert_actions,
            convert_actions_empty,
            uuid,
        )
        assert file.convert_access == convert_actions.get(file.puid)
        assert file.convert_statutory is None
