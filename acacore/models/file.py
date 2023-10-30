# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import re
from enum import Enum
from pathlib import Path
from typing import Optional
from typing import Tuple

from pydantic import UUID4
from pydantic import Field

from acacore.models.reference_files import CustomSignature
from acacore.siegfried.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.utils.io import size_fmt

from .base import ACABase
from .identification import Identification


class Action(Enum):
    CONVERT = "CONVERT"  # To convert.
    REPLACE = "REPLACE"  # Replace with template. File is not preservable.
    MANUAL = "MANUAL"  # File should be converted manually. [info about the manual conversion from reference_files].
    RENAME = "RENAME"  # File has extension mismatch. Should be renamed


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
    action: Optional[Action] = None

    def identify(self, sf: Siegfried) -> SiegfriedFile:
        """Identify the file using `siegfried`.

        Args:
            sf (Siegfried): A Siegfried class object

        Returns:
            SiegfriedFile: A dataclass object containing the results from the identification
        """
        return sf.identify(self.get_absolute_path())[0]

    def re_identify_with_aca(self, costum_sigs: list[CustomSignature]) -> None:
        """Uses the BOF and EOF to try to determine a ACAUID for the file.

        The costum_sigs list should be found on the `reference_files` repo.
        If no match can be found, the method does nothing.

        Args:
            costum_sigs: A list of the costum_signatures that the file should be checked against
        """
        bof, eof = self.get_bof_and_eof()
        # We have to go through all of the signatures in order to check their BOF en EOF with the file.
        for sig in costum_sigs:
            if sig.bof and sig.eof:
                bof_pattern = re.compile(sig.bof)
                eof_pattern = re.compile(sig.eof)
                if sig.operator == "OR":
                    if bof_pattern.search(bof) or eof_pattern.search(eof):
                        self.puid = sig.puid
                        self.signature = sig.signature
                elif sig.operator == "AND" and bof_pattern.search(bof) and eof_pattern.search(eof):
                    self.puid = sig.puid
                    self.signature = sig.signature
            elif sig.bof:
                bof_pattern = re.compile(sig.bof)
                if bof_pattern.search(bof):
                    self.puid = sig.puid
                    self.signature = sig.signature
            elif sig.eof:
                eof_pattern = re.compile(sig.eof)
                if eof_pattern.search(eof):
                    self.puid = sig.puid
                    self.signature = sig.signature

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
        return size_fmt(self.get_absolute_path().stat().st_size)

    def get_bof_and_eof(self) -> Tuple[str, str]:
        """Get the first and last kilobyte of the file.

        Returns:
            Tuple[str,str]: BOF and then EOF as `str`.
        """
        file = self.get_absolute_path()
        with file.open("rb") as file_bytes:
            # BOF
            bof = file_bytes.read(1024).hex()
            # Navigate to EOF
            try:
                file_bytes.seek(-1024, 2)
            except OSError:
                # File too small :)
                file_bytes.seek(-file_bytes.tell(), 2)
            eof = file_bytes.read(1024).hex()
        return bof, eof


class ArchiveFile(Identification, File):
    """ArchiveFile data model."""


class ConvertedFile(ACABase):
    file_id: int = Field(primary_key=True)
    uuid: UUID4 = Field(primary_key=True)
    status: str
