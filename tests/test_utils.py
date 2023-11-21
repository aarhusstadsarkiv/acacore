from pathlib import Path
from re import match

import pytest

from acacore.utils.functions import file_checksum
from acacore.utils.functions import image_size
from acacore.utils.functions import is_binary
from acacore.utils.functions import or_none
from acacore.utils.functions import rm_tree
from acacore.utils.helpers import ExceptionManager
from acacore.utils.io import size_fmt
from acacore.utils.log import setup_logger


def test_functions_or_none():
    func = or_none(lambda _: 5)
    assert func(1) == 5
    assert func(None) is None


def test_functions_file_checksum(test_files: Path, test_files_data: dict[str, dict]):
    for filename, filedata in test_files_data.items():
        assert file_checksum(test_files / filename) == filedata["checksum"]


def test_functions_is_binary(test_files: Path, test_files_data: dict[str, dict]):
    for filename, filedata in test_files_data.items():
        assert is_binary(test_files / filename) == filedata["binary"]


def test_functions_rm_tree(temp_folder: Path):
    test_folder = temp_folder.joinpath("1")
    test_folder.joinpath("2", "3").mkdir(parents=True, exist_ok=True)
    rm_tree(test_folder)
    assert not test_folder.is_dir()
    assert temp_folder.is_dir()


def test_functions_image_size(test_files: Path, test_files_data: dict[str, dict]):
    for filename, filedata in test_files_data.items():
        if filedata.get("image_size"):
            assert image_size(test_files / filename) == tuple(filedata.get("image_size"))


def test_helpers_context_manager():
    with ExceptionManager(BaseException) as context:
        raise SystemExit(3)

    assert isinstance(context.exception, SystemExit)
    assert context.exception.code == 3
    assert context.traceback is not None

    with (
        pytest.raises(KeyboardInterrupt) as raises,
        ExceptionManager(Exception) as context,
    ):
        raise KeyboardInterrupt

    assert isinstance(raises.value, KeyboardInterrupt)
    assert isinstance(context.exception, KeyboardInterrupt)
    assert context.traceback is not None

    with (
        pytest.raises(OSError) as raises,  # noqa: PT011
        ExceptionManager(BaseException, allow=[OSError]) as context,
    ):
        raise FileNotFoundError

    assert isinstance(raises.value, FileNotFoundError)
    assert isinstance(context.exception, FileNotFoundError)

    with ExceptionManager(BaseException, FileNotFoundError, allow=[OSError]) as context:
        raise FileNotFoundError

    assert isinstance(context.exception, FileNotFoundError)

    with ExceptionManager() as context:
        pass

    assert context.exception is None
    assert context.traceback is None


def test_io_size_fmt():
    assert size_fmt(2) == "2.0 B"
    assert size_fmt(2**10) == "1.0 KiB"
    assert size_fmt(2**20) == "1.0 MiB"
    assert size_fmt(2**30) == "1.0 GiB"
    assert size_fmt(2**40) == "1.0 TiB"
    assert size_fmt(2**12 + 128) == "4.1 KiB"


def test_log_setup_logger(temp_folder: Path):
    log_file: Path = temp_folder / "test.log"
    logger = setup_logger("test", files=[log_file])
    logger.info("test info message")
    logger.warning("test warning message")
    logger.error("test error message")
    log_lines: list[str] = log_file.read_text().strip().splitlines()

    assert match(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d INFO: test info message", log_lines[0])
    assert match(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d WARNING: test warning message", log_lines[1])
    assert match(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d ERROR: test error message", log_lines[2])
