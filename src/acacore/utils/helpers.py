from collections.abc import Sequence
from types import TracebackType


class ExceptionManager:
    """
    A context manager class that catches specified exceptions and stores the exception and traceback for later use.

    Exceptions whose class is explicitly declared in the 'catch' argument are always caught, even if they subclass from
    classes passed int the 'allow' argument.

    :ivar exception: The exception that was raised within the context, if any.
    :ivar traceback: The traceback associated with the exception, if any.
    :ivar catch: Tuple of exceptions that should be caught instead of letting them rise.
    :ivar allow: Tuple of exceptions that should be allowed to rise.
    """

    __slots__ = ("allow", "catch", "exception", "traceback")

    def __init__(
        self,
        *catch: type[BaseException],
        allow: Sequence[type[BaseException]] | None = None,
    ) -> None:
        """
        :param allow: Defaults to None.
        :param catch: Exception types that should be caught and not allowed to rise.
        """  # noqa: D205
        self.exception: BaseException | None = None
        self.traceback: TracebackType | None = None
        self.catch: tuple[type[BaseException], ...] = catch
        self.allow: tuple[type[BaseException], ...] = tuple(allow or [])

    def __enter__(self) -> "ExceptionManager":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.exception = exc_val
        self.traceback = exc_tb

        if not exc_type:
            return False

        return any(issubclass(exc_type, e) for e in self.catch) and (
            exc_type in self.catch or not any(issubclass(exc_type, e) for e in self.allow)
        )
