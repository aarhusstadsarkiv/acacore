from os import environ
from pathlib import Path

import pytest

from acacore.exceptions.files import IdentificationError
from acacore.siegfried import Siegfried


@pytest.fixture()
def siegfried_folder(temp_folder: Path) -> Path:
    return temp_folder / "siegfried"


@pytest.fixture()
def siegfried(siegfried_folder: Path) -> Siegfried:
    return Siegfried(Path(environ["GOPATH"], "bin", "sf"), "pronom.sig", siegfried_folder)


def test_fail(siegfried: Siegfried):
    with pytest.raises(IdentificationError):
        siegfried.run("-version")


def test_update(siegfried: Siegfried, siegfried_folder: Path):
    siegfried.update("pronom")
    assert siegfried_folder.joinpath("pronom.sig").is_file()
    assert siegfried.signature == "pronom.sig"

    siegfried.update("loc")
    assert siegfried_folder.joinpath("loc.sig").is_file()
    assert siegfried.signature == "loc.sig"

    siegfried.update("tika")
    assert siegfried_folder.joinpath("tika.sig").is_file()
    assert siegfried.signature == "tika.sig"

    siegfried.update("freedesktop")
    assert siegfried_folder.joinpath("freedesktop.sig").is_file()
    assert siegfried.signature == "freedesktop.sig"

    siegfried.update("pronom-tika-loc")
    assert siegfried_folder.joinpath("pronom-tika-loc.sig").is_file()
    assert siegfried.signature == "pronom-tika-loc.sig"

    siegfried.update("deluxe")
    assert siegfried_folder.joinpath("deluxe.sig").is_file()
    assert siegfried.signature == "deluxe.sig"

    siegfried_folder.joinpath("pronom.sig").unlink(missing_ok=True)

    siegfried.signature = "pronom"
    siegfried.update()
    assert siegfried_folder.joinpath("pronom.sig").is_file()

    siegfried.update("loc", set_signature=False)
    assert siegfried.signature == "pronom.sig"

    # TODO: add archivematica


def test_identify(siegfried: Siegfried, test_files: Path, test_files_data: dict[str, dict]):
    for filename, filedata in test_files_data.items():
        result = siegfried.identify(test_files / filename).files[0]
        assert result.filesize == filedata["filesize"]
        assert result.matches
        assert result.matches[0].model_dump() == filedata["matches"]
        assert result.best_match().model_dump() == filedata["matches"]


def test_identify_many(siegfried: Siegfried, test_files: Path, test_files_data: dict[str, dict]):
    results = siegfried.identify_many([test_files / name for name in test_files_data])
    for [_, result], [filename, filedata] in zip(results, test_files_data.items()):
        assert result.filename == str(test_files / filename)
        assert result.filesize == filedata["filesize"]
        assert result.matches
        assert result.matches[0].model_dump() == filedata["matches"]
        assert result.best_match().model_dump() == filedata["matches"]
