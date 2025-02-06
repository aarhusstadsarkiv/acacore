from .base import AcacoreError


class IdentificationError(AcacoreError):
    """Implements an error to raise if identification or related functionality fails."""


class ImageIdentificationError(AcacoreError):
    """Implements an error to raise if identification or related functionality fails."""


class FileCollectionError(AcacoreError):
    """Implements an error to raise if File discovery/collection or related functionality fails."""


class FileParseError(AcacoreError):
    """Implements an error to raise if file parsing fails."""
