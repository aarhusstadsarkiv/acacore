# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from re import compile as re_compile
from typing import Literal
from typing import Optional
from typing import Union

from pydantic import UUID4
from pydantic import Field
from typing_extensions import TypedDict

from acacore.models.reference_files import CustomSignature
from acacore.siegfried.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.utils.functions import file_checksum
from acacore.utils.functions import get_bof
from acacore.utils.functions import get_eof

from .base import ACABase
from .identification import Identification


class ActionConvert(TypedDict):
    type: Literal["CONVERT"]
    converter: str
    outputs: list[str]


class ActionExtract(TypedDict):
    type: Literal["EXTRACT"]
    tool: str
    output: str


class ActionReplace(TypedDict):
    type: Literal["REPLACE"]
    template: str


class ActionManual(TypedDict):
    type: Literal["MANUAL"]
    reasoning: str
    process: str


class ActionRename(TypedDict):
    type: Literal["RENAME"]
    new_name: str


class ActionIgnore(TypedDict):
    type: Literal["IGNORE"]
    reasoning: str


TAction = Union[ActionConvert, ActionExtract, ActionReplace, ActionManual, ActionRename, ActionIgnore]


# -----------------------------------------------------------------------------
# Model
# -----------------------------------------------------------------------------
class File(ACABase):
    """File data model."""

    uuid: UUID4 = Field(primary_key=True)
    checksum: str
    puid: Optional[str]
    relative_path: Path
    is_binary: bool
    file_size_in_bytes: int
    signature: Optional[str]
    warning: Optional[str] = None
    action: list[TAction] = Field(default_factory=lambda: [])
    root: Optional[Path] = Field(None, ignore=True)

    def get_checksum(self) -> str:
        self.checksum = file_checksum(self.get_absolute_path(self.root))
        return self.checksum

    def identify(self, sf: Siegfried) -> SiegfriedFile:
        """Identify the file using `siegfried`.

        Args:
            sf (Siegfried): A Siegfried class object

        Returns:
            SiegfriedFile: A dataclass object containing the results from the identification
        """
        return sf.identify(self.get_absolute_path(self.root)).files[0]

    def identify_custom(self, custom_sigs: list[CustomSignature]) -> Optional[CustomSignature]:
        """Uses the BOF and EOF to try to determine a ACAUID for the file.

        The custom_sigs list should be found on the `reference_files` repo.
        If no match can be found, the method does nothing.

        Args:
            custom_sigs: A list of the custom_signatures that the file should be checked against
        """
        bof = get_bof(self.get_absolute_path(self.root)).hex()
        eof = get_eof(self.get_absolute_path(self.root)).hex()
        signature: Optional[CustomSignature] = None
        signature_length: int = 0

        # We have to go through all the signatures in order to check their BOF en EOF with the file.
        for sig in custom_sigs:
            if sig.bof and sig.eof:
                bof_pattern, eof_pattern = re_compile(sig.bof), re_compile(sig.eof)
                match_bof = bof_pattern.search(bof)
                match_eof = eof_pattern.search(eof)
                match_length = (match_bof.end() - match_bof.start()) if match_bof else 0
                match_length += (match_eof.end() - match_eof.start()) if match_eof else 0
                if sig.operator == "OR" and (match_bof or match_eof) and match_length > signature_length:
                    signature = sig
                elif sig.operator == "AND" and match_bof and match_eof and match_length > signature_length:
                    signature = sig
            elif sig.bof:
                match_bof = re_compile(sig.bof).search(bof)
                match_length = (match_bof.end() - match_bof.start()) if match_bof else 0
                signature = sig if match_bof and match_length > signature_length else signature
            elif sig.eof:
                match_eof = re_compile(sig.eof).search(eof)
                match_length = (match_eof.end() - match_eof.start()) if match_eof else 0
                signature = sig if match_eof and match_length > signature_length else signature

        return signature

    def get_absolute_path(self, root: Optional[Path] = None) -> Path:
        return root.joinpath(self.relative_path) if root else self.relative_path.resolve()

    def name(self) -> str:
        """
        Get the file name.

        Returns:
        -------
        str
            File name.
        """
        return self.relative_path.name

    def suffix(self) -> str:
        """
        Get the file extension.

        Returns:
        -------
        str
            File extension.
        """
        return self.relative_path.suffix.lower()


class ArchiveFile(Identification, File):
    """ArchiveFile data model."""


class ConvertedFile(ACABase):
    file_id: int = Field(primary_key=True)
    uuid: UUID4 = Field(primary_key=True)
    status: str
