from pathlib import Path
from re import compile as re_compile
from typing import Optional
from uuid import uuid4

from pydantic import UUID4
from pydantic import Field

from acacore.models.reference_files import CustomSignature
from acacore.siegfried.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.utils.functions import file_checksum
from acacore.utils.functions import get_bof
from acacore.utils.functions import get_eof
from acacore.utils.functions import is_binary

from .base import ACABase
from .identification import Identification
from .reference_files import Action
from .reference_files import TActionType


class File(ACABase):
    """
    File model containing all information used by the rest of the archival suite of tools.

    Attributes:
        uuid (UUID4): The UUID of the file.
        checksum (str): The checksum of the file.
        puid (Optional[str]): The PUID (PRONOM Unique Identifier) of the file.
        relative_path (Path): The relative path to the file.
        is_binary (bool): Indicates whether the file is binary.
        size (int): The size of the file.
        signature (Optional[str]): The signature of the file.
        warning (Optional[str]): Any warning associated with the file PUID.
        action (list[TAction]): A list of actions related to the file.
        root (Optional[Path]): The root directory for the file.
    """

    uuid: UUID4 = Field(default_factory=uuid4)
    checksum: str
    puid: Optional[str]
    relative_path: Path = Field(primary_key=True)
    is_binary: bool
    size: int
    signature: Optional[str]
    warning: Optional[list[str]] = None
    action: Optional[TActionType] = None
    action_data: Optional[Action] = None
    root: Optional[Path] = Field(None, ignore=True)

    @classmethod
    def from_file(cls, path: Path, root: Optional[Path] = None):
        """
        Create a File object from a given file.

        Args:
            path: The path to the file.
            root: Optionally, the root to be used to compute the relative path to the file

        Returns:
            File: A File object.
        """
        return cls(
            checksum=file_checksum(path),
            puid=None,
            relative_path=path.relative_to(root) if root else path,
            is_binary=is_binary(path),
            size=path.stat().st_size,
            signature=None,
            warning=None,
            action=None,
            root=root,
        )

    def identify(self, sf: Siegfried, *, set_match: bool = False) -> SiegfriedFile:
        """
        Identify the file using `siegfried`.

        Args:
            sf (Siegfried): A Siegfried class object
            set_match (bool): Set results of Siegfried match if True

        Returns:
            SiegfriedFile: A dataclass object containing the results from the identification
        """
        result = sf.identify(self.get_absolute_path(self.root)).files[0]
        match = result.best_match()
        if set_match:
            self.puid = match.id if match else None
            self.signature = match.format if match else None
            self.warning = match.warning if match else None
        return result

    def identify_custom(
        self,
        custom_signatures: list[CustomSignature],
        *,
        set_match: bool = False,
    ) -> Optional[CustomSignature]:
        """
        Uses the BOF and EOF to try to determine a ACAUID for the file.

        The custom_sigs list should be found on the `reference_files` repo.
        If no match can be found, the method does nothing.

        Args:
            custom_signatures: A list of the custom_signatures that the file should be checked against
            set_match (bool): Set results of match if True
        """
        bof = get_bof(self.get_absolute_path(self.root)).hex()
        eof = get_eof(self.get_absolute_path(self.root)).hex()
        signature: Optional[CustomSignature] = None
        signature_length: int = 0

        # We have to go through all the signatures in order to check their BOF en EOF with the file.
        for sig in custom_signatures:
            if sig.bof and sig.eof:
                bof_pattern, eof_pattern = re_compile(sig.bof), re_compile(sig.eof)
                match_bof = bof_pattern.search(bof)
                match_eof = eof_pattern.search(eof)
                match_length = (match_bof.end() - match_bof.start()) if match_bof else 0
                match_length += (match_eof.end() - match_eof.start()) if match_eof else 0
                if sig.operator == "OR":
                    signature = sig if (match_bof or match_eof) and match_length > signature_length else signature
                elif sig.operator == "AND":
                    signature = sig if match_bof and match_eof and match_length > signature_length else signature
            elif sig.bof:
                match_bof = re_compile(sig.bof).search(bof)
                match_length = (match_bof.end() - match_bof.start()) if match_bof else 0
                signature = sig if match_bof and match_length > signature_length else signature
            elif sig.eof:
                match_eof = re_compile(sig.eof).search(eof)
                match_length = (match_eof.end() - match_eof.start()) if match_eof else 0
                signature = sig if match_eof and match_length > signature_length else signature

        if set_match:
            self.puid = signature.puid if signature else None
            self.signature = signature.signature if signature else None
            self.warning = None

        return signature

    def get_action(self, actions: dict[str, Action]) -> Optional[Action]:
        action: Optional[Action] = actions.get(self.puid)
        self.action, self.action_data = action.action if action else None, action
        return action

    def get_absolute_path(self, root: Optional[Path] = None) -> Path:
        return root.joinpath(self.relative_path) if root else self.relative_path.resolve()

    def get_checksum(self) -> str:
        self.checksum = file_checksum(self.get_absolute_path(self.root))
        return self.checksum

    def get_size(self) -> int:
        self.size = self.get_absolute_path(self.root).stat().st_size
        return self.size

    @property
    def name(self) -> str:
        """
        Get file name.

        Returns:
            str: File name.
        """
        return self.relative_path.name

    @name.setter
    def name(self, new_name: str):
        self.relative_path = self.relative_path.with_name(new_name).with_suffix(self.suffix)

    @property
    def suffix(self) -> str:
        """
        Get file suffix.

        Returns:
            str: File extension.
        """
        return self.relative_path.suffix.lower()

    @suffix.setter
    def suffix(self, new_suffix: str):
        self.relative_path = self.relative_path.with_suffix(new_suffix)


class ArchiveFile(Identification, File):
    """ArchiveFile data model."""


class ConvertedFile(ACABase):
    file_id: int = Field(primary_key=True)
    uuid: UUID4 = Field(primary_key=True)
    status: str
