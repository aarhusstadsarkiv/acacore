from types import TracebackType
from typing import Optional
from typing import Sequence
from typing import Type


class ExceptionManager:
    """
    A context manager class that catches specified exceptions and stores the exception and traceback for later use.

    Exceptions whose class is explicitly declared in the 'catch' argument are always caught,
    even if they subclass from classes passed int the 'allow' argument.

    Args:
        *catch (Type[BaseException]): Exception types that should be caught and not allowed to rise.

    Attributes:
        exception (Optional[BaseException]): The exception that was raised within the context, if any.
        traceback (Optional[TracebackType]): The traceback associated with the exception, if any.
        catch (tuple[Type[BaseException], ...]): Tuple of exceptions that should be caught instead of letting them rise.
        allow (tuple[Type[BaseException], ...]): Tuple of exceptions that should be allowed to rise.
    """

    __slots__ = ("exception", "traceback", "catch", "allow")

    def __init__(self, *catch: Type[BaseException], allow: Optional[Sequence[Type[BaseException]]] = None) -> None:
        self.exception: Optional[BaseException] = None
        self.traceback: Optional[TracebackType] = None
        self.catch: tuple[Type[BaseException], ...] = catch
        self.allow: tuple[Type[BaseException], ...] = tuple(allow or [])

    def __enter__(self) -> "ExceptionManager":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.exception = exc_val
        self.traceback = exc_tb

        if not exc_type:
            return False

        return any(issubclass(exc_type, e) for e in self.catch) and (
            exc_type in self.catch or not any(issubclass(exc_type, e) for e in self.allow)
        )
