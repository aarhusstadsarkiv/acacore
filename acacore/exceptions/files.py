from .base import ACAException


class IdentificationError(ACAException):
    """Implements an error to raise if identification or related functionality fails."""


class FileCollectionError(ACAException):
    """Implements an error to raise if File discovery/collection or related functionality fails."""


class FileParseError(ACAException):
    """Implements an error to raise if file parsing fails."""
