# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------


import os
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from pydantic import UUID4, Field, validator

from .base import ACABase
from .identification import Identification
from ..utils.io import size_fmt


# -----------------------------------------------------------------------------
# Model
# -----------------------------------------------------------------------------


class File(ACABase):
    """File data model."""

    uuid: UUID4 = Field(None)
    checksum: Optional[str]
    relative_path: Path = Field(None)
    is_binary: bool = Field(None)
    file_size_in_bytes: int = Field(None)

    # Validators
    @validator("relative_path")
    def path_must_be_file(cls, path: Path) -> Path:
        """Resolves the file path and validates that it points to an existing file."""
        if "ROOTPATH" in os.environ:
            if os.name == "posix":
                posix_path = str(path).replace("\\", "/")
                absolute_path = Path(os.environ["ROOTPATH"], posix_path)
                # print(absolute_path)
            else:
                absolute_path = Path(os.environ["ROOTPATH"], path)
            if not absolute_path.resolve().is_file():
                raise ValueError(f"File with path {absolute_path} does not exist")
        else:
            if not path.resolve().is_file():
                raise ValueError(f"File with path {path} does not exist")
        if os.name == "posix":
            return Path(str(path).replace("\\", "/"))
        else:
            return path

    @validator("uuid", pre=True, always=True)
    def set_uuid(cls, uuid: UUID4) -> UUID:
        return uuid or uuid4()

    def get_absolute_path(self) -> Path:
        return Path(os.environ["ROOTPATH"], self.relative_path)

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

    def size(self) -> str:
        """Get the file size in human readable string format.

        Returns:
        -------
        str
            File size in human readable format.
        """
        return str(size_fmt(self.get_absolute_path().stat().st_size))

    def size_as_int(self) -> int:
        """Get the file size in human readable string format.

        Returns:
        -------
        int
            File size in human readable format.
        """
        return self.get_absolute_path().stat().st_size


class ArchiveFile(Identification, File):
    """ArchiveFile data model."""
