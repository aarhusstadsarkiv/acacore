from pathlib import Path
from re import match

from acacore.utils.functions import file_checksum
from acacore.utils.functions import image_size
from acacore.utils.functions import is_binary
from acacore.utils.functions import or_none
from acacore.utils.functions import rm_tree
from acacore.utils.io import size_fmt
from acacore.utils.log import setup_logger


def test_functions(test_files: Path, test_files_data: dict[str, dict], temp_folder: Path):
    # or_none
    func = or_none(lambda _: 5)
    assert func(1) == 5
    assert func(None) is None

    # file_checksum
    for filename, filedata in test_files_data.items():
        assert file_checksum(test_files / filename) == filedata["checksum"]

    # is_binary
    for filename, filedata in test_files_data.items():
        assert is_binary(test_files / filename) == filedata["binary"]

    # rm_tree
    test_folder = temp_folder.joinpath("1")
    test_folder.joinpath("2", "3").mkdir(parents=True, exist_ok=True)
    rm_tree(test_folder)
    assert not test_folder.is_dir()
    assert temp_folder.is_dir()

    # image_size
    for filename, filedata in test_files_data.items():
        if filedata.get("image_size"):
            assert image_size(test_files / filename) == tuple(filedata.get("image_size"))


def test_io():
    # size_fmt
    assert size_fmt(2) == "2.0 B"
    assert size_fmt(2**10) == "1.0 KiB"
    assert size_fmt(2**20) == "1.0 MiB"
    assert size_fmt(2**30) == "1.0 GiB"
    assert size_fmt(2**40) == "1.0 TiB"
    assert size_fmt(2**12 + 128) == "4.1 KiB"


def test_log(temp_folder: Path):
    log_file: Path = temp_folder / "test.log"
    logger = setup_logger("test", files=[log_file])
    logger.info("test info message")
    logger.warning("test warning message")
    logger.error("test error message")
    log_lines: list[str] = log_file.read_text().strip().splitlines()

    assert match(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d INFO: test info message", log_lines[0])
    assert match(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d WARNING: test warning message", log_lines[1])
    assert match(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d ERROR: test error message", log_lines[2])
