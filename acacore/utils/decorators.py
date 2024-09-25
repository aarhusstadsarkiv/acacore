from typing import Any
from typing import Callable


def docstring_format(**kwargs: Any) -> Callable[[Callable], Callable]:  # noqa: ANN401
    def decorator(func: Callable) -> Callable:
        func.__doc__ = (func.__doc__ or "").format(**kwargs)
        return func

    return decorator
