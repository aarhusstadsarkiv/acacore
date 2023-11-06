from pathlib import Path
from re import compile as re_compile
from typing import Optional
from uuid import uuid4

from pydantic import Field
from pydantic import UUID4

from acacore.exceptions.files import IdentificationError
from acacore.siegfried.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.utils.functions import file_checksum
from acacore.utils.functions import get_bof
from acacore.utils.functions import get_eof
from acacore.utils.functions import image_size
from acacore.utils.functions import is_binary

from .base import ACABase
from .identification import Identification
from .reference_files import Action
from .reference_files import ActionData
from .reference_files import CustomSignature
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
    relative_path: Path = Field(primary_key=True)
    is_binary: bool
    size: int
    puid: Optional[str]
    signature: Optional[str]
    warning: Optional[list[str]] = None
    action: Optional[TActionType] = None
    action_data: Optional[ActionData] = None
    root: Optional[Path] = Field(None, ignore=True)

    @classmethod
    def from_file(
        cls,
        path: Path,
        root: Optional[Path] = None,
        siegfried: Optional[Siegfried] = None,
        actions: Optional[dict[str, Action]] = None,
        custom_signatures: Optional[list[CustomSignature]] = None,
    ):
        """
        Create a File object from a given file.

        Given a Siegfried object, the file will be identified.

        Given a dictionary of Actions, the file action properties will be set.

        Given a list of CustomSignatures, the file identification will be refined.

        Args:
            path: The path to the file.
            root: Optionally, the root to be used to compute the relative path to the file.
            siegfried: A Siegfried class object to identify the file.
            actions: A dictionary with PUID keys and Action values to assign an action.
            custom_signatures: A list of CustomSignature objects to refine the identification.

        Returns:
            File: A File object.
        """
        file = cls(
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

        if siegfried:
            file.identify(siegfried, set_match=True)

        if custom_signatures and not file.puid:
            file.identify_custom(custom_signatures, set_match=True)

        if actions:
            file.get_action(actions)

        if custom_signatures and file.action == "reidentify":
            custom_match = file.identify_custom(custom_signatures)
            if custom_match:
                file.puid = custom_match.puid
                file.signature = custom_match.signature
                file.warning = None
                file.get_action(actions)
            elif file.action_data.reidentify and file.action_data.reidentify.onfail:
                file.action = file.action_data.reidentify.onfail
            else:
                file.action = "manual"
                file.warning = [*(file.warning or []), repr(IdentificationError("Re-identify failure"))]

        if file.action_data and file.action_data.ignore and file.action_data.ignore.ignore_if:
            for ignore_if in file.action_data.ignore.ignore_if:
                if ignore_if.pixel_total or ignore_if.pixel_width or ignore_if.pixel_height:
                    width, height = image_size(file.get_absolute_path())
                    if (
                        width * height < ignore_if.pixel_total
                        or width < ignore_if.pixel_width
                        or height < ignore_if.pixel_height
                    ):
                        file.action = "ignore"
                        file.action_data.ignore.reasoning = ignore_if.reason or file.action_data.ignore.reasoning
                elif file.is_binary and file.size < (ignore_if.binary_size or 0):  # noqa: SIM114
                    file.action = "ignore"
                    file.action_data.ignore.reasoning = ignore_if.reason or file.action_data.ignore.reasoning
                elif file.size < (ignore_if.size or 0):
                    file.action = "ignore"
                    file.action_data.ignore.reasoning = ignore_if.reason or file.action_data.ignore.reasoning

        return file

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
            self.warning = (match.warning or None) if match else None
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
        self.action, self.action_data = action.action if action else None, action.action_data if action else None
        return action

    def get_absolute_path(self, root: Optional[Path] = None) -> Path:
        root = root or self.root
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
