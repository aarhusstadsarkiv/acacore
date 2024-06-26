from pathlib import Path
from re import compile as re_compile
from typing import Optional
from uuid import uuid4

from pydantic import Field
from pydantic import UUID4

from acacore.siegfried.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.siegfried.siegfried import TSiegfriedClass
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
from .reference_files import IgnoreAction
from .reference_files import IgnoreIfAction
from .reference_files import ManualAction
from .reference_files import TActionType


def _ignore_if(file: "File", ignore_ifs: list[IgnoreIfAction]) -> "File":
    action: Optional[TActionType] = None
    action_data: Optional[ActionData] = None

    for ignore_if in ignore_ifs:
        if ignore_if.pixel_total or ignore_if.pixel_width or ignore_if.pixel_height:
            width, height = image_size(file.get_absolute_path())
            if (
                width * height < (ignore_if.pixel_total or 0)
                or width < (ignore_if.pixel_width or 0)
                or height < (ignore_if.pixel_height or 0)
            ):
                action = "ignore"
        elif ignore_if.binary_size and file.is_binary and file.size < ignore_if.binary_size:  # noqa: SIM114
            action = "ignore"
        elif ignore_if.size and file.size < ignore_if.size:
            action = "ignore"

        if action:
            action_data = file.action_data or ActionData()
            action_data.ignore = action_data.ignore or IgnoreAction()
            action_data.ignore.reason = ignore_if.reason or action_data.ignore.reason
            break

    if action and action_data:
        file.action = action
        file.action_data = action_data

    return file


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
        action (Optional[str]): The name of the main action for the file's PUID, if one exists.
        action_data (Optional[ActionData]): The data for the action for the file's PUID, if one exists.
        processed (bool): True if the file has been processed, false otherwise.
        root (Optional[Path]): The root directory for the file.
    """

    uuid: UUID4 = Field(default_factory=uuid4, index=["idx_uuid"])
    checksum: str = Field(index=["idx_checksum"])
    relative_path: Path = Field(primary_key=True)
    is_binary: bool
    size: int
    puid: Optional[str]
    signature: Optional[str]
    warning: Optional[list[str]] = None
    action: Optional[TActionType] = None
    action_data: Optional[ActionData] = None
    processed: bool = False
    root: Optional[Path] = Field(None, ignore=True)

    @classmethod
    def from_file(
        cls,
        path: Path,
        root: Optional[Path] = None,
        siegfried: Optional[Siegfried] = None,
        actions: Optional[dict[str, Action]] = None,
        custom_signatures: Optional[list[CustomSignature]] = None,
        *,
        uuid: Optional[UUID4] = None,
        processed: bool = False,
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
            uuid: Optionally, a specific UUID to use for the file.
            processed: Optionally, the value to be used for the processed property.

        Returns:
            File: A File object.
        """
        file = cls(
            uuid=uuid or uuid4(),
            checksum=file_checksum(path),
            puid=None,
            relative_path=path.relative_to(root) if root else path,
            is_binary=is_binary(path),
            size=path.stat().st_size,
            signature=None,
            warning=None,
            action=None,
            root=root,
            processed=processed,
        )
        match_classes: list[TSiegfriedClass] = []

        if siegfried:
            siegfried_match = file.identify(siegfried, set_match=True).best_match()
            match_classes.extend(siegfried_match.match_class if siegfried_match else [])

        if custom_signatures and not file.puid:
            file.identify_custom(custom_signatures, set_match=True)

        if actions:
            file.get_action(actions, match_classes)

        if custom_signatures and file.action == "reidentify":
            custom_match = file.identify_custom(custom_signatures)
            if custom_match:
                file.puid = custom_match.puid
                file.signature = custom_match.signature
                file.warning = []
                if custom_match.extension and file.suffix != custom_match.extension:
                    file.warning.append("extension mismatch")
                file.warning = file.warning or None
                file.get_action(actions, match_classes)
            elif file.action_data.reidentify and file.action_data.reidentify.onfail:
                file.action = file.action_data.reidentify.onfail
            else:
                file.action = "manual"
                file.action_data = ActionData(manual=ManualAction(reason="Re-identify failure", process=""))
                file.puid = file.signature = file.warning = None

        if file.action_data and file.action_data.ignore:
            file = _ignore_if(file, file.action_data.ignore.ignore_if)

        if file.action != "ignore" and actions and "*" in actions:
            file = _ignore_if(file, actions["*"].ignore.ignore_if if actions["*"].ignore else [])

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

    def get_action(
        self,
        actions: dict[str, Action],
        match_classes: Optional[list[TSiegfriedClass]] = None,
    ) -> Optional[Action]:
        action: Optional[Action] = None

        identifiers: list[str] = [
            self.puid,
            *(match_classes or []),
        ]
        if self.suffix:
            identifiers.append(f"!ext={''.join(self.get_absolute_path().suffixes)}")
        if self.is_binary:
            identifiers.append("!binary")
        if not self.size:
            identifiers.insert(0, "!empty")

        for identifier in identifiers:
            action = actions.get(identifier)
            if action:
                break

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
