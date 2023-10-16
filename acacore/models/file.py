# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic import UUID4

from acacore.utils.io import size_fmt
from .base import ACABase
from .identification import Identification


# -----------------------------------------------------------------------------
# Model
# -----------------------------------------------------------------------------


class File(ACABase):
    """File data model."""

    id: int = Field(primary_key=True)  # noqa: A003
    uuid: UUID4 = Field(primary_key=True)
    checksum: str
    puid: Optional[str]
    relative_path: Path
    is_binary: bool
    file_size_in_bytes: int
    signature: Optional[str]
    warning: Optional[str] = None
    action: Optional[str] = None

    def get_absolute_path(self, root: Optional[Path] = None) -> Path:
        return root.joinpath(self.relative_path) if root else self.relative_path.resolve()

    def read_text(self) -> str:
        """Expose read text functionality from pathlib.

        Encoding is set to UTF-8.

        Returns:
        -------
        str
            File text data.
        """
        return self.get_absolute_path().read_text(encoding="utf-8")

    def read_bytes(self) -> bytes:
        """Expose read_bytes() functionality from pathlib.

        Returns:
        -------
        bytes
            File byte data.
        """  # noqa: D402
        return self.get_absolute_path().read_bytes()

    def name(self) -> str:
        """Get the file name.

        Returns:
        -------
        str
            File name.
        """
        return self.relative_path.name

    def ext(self) -> str:
        """Get the file extension.

        Returns:
        -------
        str
            File extension.
        """
        return self.relative_path.suffix.lower()

    def size(self) -> int:
        """Get the file size in bytes.

        Returns:
        -------
        int
            File size in bytes.
        """
        return self.get_absolute_path().stat().st_size

    def size_fmt(self) -> str:
        """Get the file size in a human-readable string format.

        Returns:
        -------
        str
            File size in human-readable format.
        """
        return str(size_fmt(self.get_absolute_path().stat().st_size))


class ArchiveFile(Identification, File):
    """ArchiveFile data model."""


class ConvertedFile(ACABase):
    file_id: int = Field(primary_key=True)
    uuid: UUID4 = Field(primary_key=True)
    status: str
