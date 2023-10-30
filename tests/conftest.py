from json import loads
from pathlib import Path

import pytest

from acacore.utils.functions import rm_tree


@pytest.fixture(scope="session")
def test_folder() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope="session")
def temp_folder(test_folder: Path) -> Path:
    return test_folder / "tmp"


@pytest.fixture(scope="session")
def test_files(test_folder: Path) -> Path:
    return test_folder / "files"


@pytest.fixture(scope="session")
def test_files_data(test_files: Path) -> dict[str, dict]:
    return loads(test_files.joinpath("files.json").read_text())


@pytest.fixture(autouse=True, scope="session")
def _pre_test(temp_folder: Path):
    rm_tree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)
