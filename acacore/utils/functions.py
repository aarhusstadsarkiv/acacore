from hashlib import sha256
from pathlib import Path
from typing import Callable
from typing import Optional
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


_text_bytes: bytes = bytes([7, 8, 9, 10, 12, 13, 27, *range(0x20, 0x7F), *range(0x80, 0x100)])


def or_none(func: Callable[[T], R]) -> Callable[[T], Optional[R]]:
    """Create a lambda function of arity one that will return None if its argument is None.

    Otherwise will call func on the object.

    Args:
        func: A function of type (T) -> R that will handle the object if it is not none.

    Returns:
        object: A function of type (T) -> R | None.
    """
    return lambda x: None if x is None else func(x)


def rm_tree(path: Path):
    if not path.is_dir():
        path.unlink(missing_ok=True)
        return

    for item in path.iterdir():
        rm_tree(item) if item.is_dir() else item.unlink(missing_ok=True)

    path.rmdir()


def file_checksum(path: Path) -> str:
    file_hash = sha256()
    with path.open("rb") as f:
        chunk = f.read(2**20)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(2**20)
    return file_hash.hexdigest()


def is_binary(path: Path, chunk_size: int = 1024):
    with path.open("rb") as f:
        return bool(f.read(chunk_size).translate(None, _text_bytes))


def get_bof(path: Path, chunk_size: int = 1024) -> bytes:
    with path.open("rb") as f:
        return f.read(chunk_size)


def get_eof(path: Path, chunk_size: int = 1024) -> bytes:
    with path.open("rb") as f:
        f.seek(path.stat().st_size - chunk_size)
        return f.read(chunk_size)
