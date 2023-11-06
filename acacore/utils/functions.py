from hashlib import sha256
from pathlib import Path
from typing import Callable
from typing import Generator
from typing import Optional
from typing import TypeVar

from PIL import Image

T = TypeVar("T")
R = TypeVar("R")

_text_bytes: bytes = bytes([7, 8, 9, 10, 12, 13, 27, *range(0x20, 0x7F), *range(0x80, 0x100)])


def or_none(func: Callable[[T], R]) -> Callable[[T], Optional[R]]:
    """Create a lambda function of arity one that will return None if its argument is None.

    Otherwise, will call func on the object.

    Args:
        func: A function of type (T) -> R that will handle the object if it is not none.

    Returns:
        object: A function of type (T) -> R | None.
    """
    return lambda x: None if x is None else func(x)


def rm_tree(path: Path):
    """
    Remove a directory and all the files and other folders it contains.

    Args:
        path (Path): The path to the directory.
    """
    if not path.is_dir():
        path.unlink(missing_ok=True)
        return

    for item in path.iterdir():
        rm_tree(item) if item.is_dir() else item.unlink(missing_ok=True)

    path.rmdir()


def find_files(*root: Path, exclude: Optional[list[Path]] = None) -> Generator[Path, None, None]:
    """
    Find files in the specified root directories, excluding any files or directories included in the `exclude` list.

    Paths in the exclude argument will be ignored, including their children if they are folders.

    Args:
        *root (Path): The root directories to search for files.
        exclude (Optional[list[Path]]): A list of files or directories to exclude from the search. Defaults to None.

    Returns:
        Generator[Path, None, None]: A generator that yields paths of found files.
    """
    exclude = exclude or []
    for f in root:
        if f in exclude:
            continue
        elif f.is_file():
            yield f
        elif f.is_dir():
            yield from find_files(*f.iterdir(), exclude=exclude)


def file_checksum(path: Path) -> str:
    """
    Calculate the checksum of a file using the SHA256 hash algorithm.

    Args:
        path (Path): The path to the file.

    Returns:
        str: The SHA256 checksum of the file in hex digest form.
    """
    file_hash = sha256()
    with path.open("rb") as f:
        chunk = f.read(2**20)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(2**20)
    return file_hash.hexdigest()


def is_binary(path: Path, chunk_size: int = 1024):
    """
    Check if a file is a binary or plain text.

    Args:
        path (Path): The path to the file to be checked.
        chunk_size (int): The size of the chunk to be read from the file in bytes. Default is 1024.

    Returns:
        bool: True if the file is binary, False if it is not.
    """
    with path.open("rb") as f:
        return bool(f.read(chunk_size).translate(None, _text_bytes))


def get_bof(path: Path, chunk_size: int = 1024) -> bytes:
    """
    Get the beginning chunk of a file in bytes.

    Args:
        path (Path): The path of the file to read.
        chunk_size (int): The size of each chunk to read from the file. Defaults to 1024.

    Returns:
        bytes: The contents of the first chunk of the file as a bytes object.
    """
    with path.open("rb") as f:
        return f.read(chunk_size)


def get_eof(path: Path, chunk_size: int = 1024) -> bytes:
    """
    Get the ending chunk of a file in bytes.

    Args:
        path (Path): The path of the file to read.
        chunk_size (int): The size of each chunk to read from the file. Defaults to 1024.

    Returns:
        bytes: The contents of the last chunk of the file as a bytes object.
    """
    with path.open("rb") as f:
        f.seek(path.stat().st_size - chunk_size)
        return f.read(chunk_size)


def image_size(path: Path) -> tuple[int, int]:
    """
    Calculate the size of an image.

    Args:
        path (Path): The path to the image file.

    Returns:
        tuple[int, int]: A tuple representing the width and height of the image.

    Raises:
        FileNotFoundError: If the provided path does not exist.
        IsADirectoryError: If the provided path points to a directory instead of a file.
        PIL.UnidentifiedImageError: If the provided file is not a valid image file.
    """
    with Image.open(path) as i:
        return i.size
