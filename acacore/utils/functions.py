from pathlib import Path
from typing import Callable
from typing import Optional
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


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
