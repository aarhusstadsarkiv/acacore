from functools import reduce
from pathlib import Path
from re import compile as re_compile
from uuid import uuid4

from pydantic import BaseModel
from pydantic import UUID4

from acacore.database.column import DBField
from acacore.siegfried.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.siegfried.siegfried import TSiegfriedFileClass
from acacore.utils.functions import file_checksum
from acacore.utils.functions import get_bof
from acacore.utils.functions import get_eof
from acacore.utils.functions import image_size
from acacore.utils.functions import is_binary

from .reference_files import Action
from .reference_files import ActionData
from .reference_files import CustomSignature
from .reference_files import IgnoreAction
from .reference_files import IgnoreIfAction
from .reference_files import ManualAction
from .reference_files import TActionType


def _ignore_if(file: "File", ignore_ifs: list[IgnoreIfAction]) -> "File":
    action: TActionType | None = None
    action_data: ActionData | None = None

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


class File(BaseModel):
    """
    File model containing all information used by the rest of the archival suite of tools.

    :ivar uuid: The UUID of the file.
    :ivar checksum: The checksum of the file.
    :ivar puid: The PUID (PRONOM Unique Identifier) of the file.
    :ivar relative_path: The relative path to the file.
    :ivar is_binary: Indicates whether the file is binary.
    :ivar size: The size of the file.
    :ivar signature: The signature of the file.
    :ivar warning: Any warning associated with the file PUID.
    :ivar action: The name of the main action for the file's PUID, if one exists.
    :ivar action_data: The data for the action for the file's PUID, if one exists.
    :ivar processed: True if the file has been processed, false otherwise.
    :ivar lock: True if the file is locked for edits, false otherwise.
    :ivar root: The root directory for the file.
    """

    uuid: UUID4 = DBField(default_factory=uuid4, index=["idx_uuid"])
    checksum: str = DBField(index=["idx_checksum"])
    relative_path: Path = DBField(primary_key=True)
    is_binary: bool
    size: int
    puid: str | None
    signature: str | None
    warning: list[str] | None = None
    action: TActionType | None = DBField(index=["idx_action"])
    action_data: ActionData | None = None
    processed: bool = False
    lock: bool = False
    root: Path | None = DBField(None, ignore=True)

    @classmethod
    def from_file(
        cls,
        path: Path,
        root: Path | None = None,
        siegfried: Siegfried | SiegfriedFile | None = None,
        actions: dict[str, Action] | None = None,
        custom_signatures: list[CustomSignature | None] | None = None,
        *,
        uuid: UUID4 | None = None,
        processed: bool = False,
    ):
        """
        Create a File object from a given file.

        Given a Siegfried object, the file will be identified.

        Given a dictionary of Actions, the file action properties will be set.

        Given a list of CustomSignatures, the file identification will be refined.

        :param path: The path to the file.
        :param root: Optionally, the root to be used to compute the relative path to the file, defaults to None.
        :param siegfried: A Siegfried or SiegfriedFile object to identify the file, defaults to None.
        :param actions: A dictionary with PUID keys and Action values to assign an action, defaults to None.
        :param custom_signatures: A list of CustomSignature objects to refine the identification, defaults to None.
        :param uuid: Optionally, a specific UUID to use for the file, defaults to None.
        :param processed: Optionally, the value to be used for the processed property, defaults to False.
        :return: A File object.
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
        file_classes: list[TSiegfriedFileClass] = []
        action: Action | None = None

        if siegfried:
            siegfried_match = file.identify(siegfried, set_match=True).best_match()
            file_classes.extend(siegfried_match.match_class if siegfried_match else [])

        if custom_signatures and not file.puid:
            file.identify_custom(custom_signatures, set_match=True)

        if actions:
            action = file.get_action(actions, file_classes)

        if custom_signatures and file.action == "reidentify":
            custom_match = file.identify_custom(custom_signatures)
            if custom_match:
                file.puid = custom_match.puid
                file.signature = custom_match.signature
                file.warning = []
                if custom_match.extension and file.suffix != custom_match.extension:
                    file.warning.append("extension mismatch")
                file.warning = file.warning or None
                action = file.get_action(actions, file_classes)
            elif file.action_data.reidentify and file.action_data.reidentify.onfail:
                file.action = file.action_data.reidentify.onfail
            else:
                action = None
                file.action = "manual"
                file.action_data = ActionData(manual=ManualAction(reason="Re-identify failure", process=""))
                file.puid = file.signature = file.warning = None

        if file.action_data and file.action_data.ignore:
            file = _ignore_if(file, file.action_data.ignore.ignore_if)

        if file.action != "ignore" and actions and "*" in actions:
            file = _ignore_if(file, actions["*"].ignore.ignore_if if actions["*"].ignore else [])

        if action and file.warning:
            file.warning = [w for w in file.warning if w.lower() not in [aw.lower() for aw in action.ignore_warnings]]
            file.warning = file.warning or None

        return file

    def identify(self, sf: Siegfried | SiegfriedFile | None, *, set_match: bool = False) -> SiegfriedFile:
        """
        Identify the file using `siegfried`.

        :param sf: A Siegfried class object.
        :param set_match: Set results of Siegfried match if True, defaults to False.
        :return: A dataclass object containing the results from the identification.
        """
        result: SiegfriedFile = sf.identify(self.get_absolute_path()).files[0] if isinstance(sf, Siegfried) else sf

        if set_match and (match := result.best_match()):
            self.puid = match.id
            self.signature = match.format
            self.warning = match.warning or None
        elif set_match:
            self.puid = self.signature = self.warning = None

        return result

    def identify_custom(
        self,
        custom_signatures: list[CustomSignature],
        *,
        set_match: bool = False,
    ) -> CustomSignature | None:
        """
        Uses the BOF and EOF to try to determine a ACAUID for the file.

        The custom_sigs list should be found on the `reference_files` repo. If no match can be found, the method does
        nothing.

        :param custom_signatures: A list of the custom_signatures that the file should be checked against.
        :param set_match: Set results of match if True, defaults to False.
        """
        bof = get_bof(self.get_absolute_path(self.root)).hex()
        eof = get_eof(self.get_absolute_path(self.root)).hex()
        signature: CustomSignature | None = None
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
        file_classes: list[TSiegfriedFileClass | None] | None = None,
        *,
        set_match: bool = True,
    ) -> Action | None:
        """
        Returns the Action matching the file.

        :param actions: A dictionary containing the available actions.
        :param file_classes: A list of file classes or None.
        :param set_match: Set the matched action if True, defaults to False.
        :return: An instance of Action or None if no action is found.
        """
        identifiers: list[str] = [self.puid or "", *(f"!{c}" for c in file_classes or [])]

        if self.suffix:
            identifiers.append(f"!ext={''.join(self.get_absolute_path().suffixes)}")
        if self.is_binary:
            identifiers.append("!binary")
        if not self.size:
            identifiers.insert(0, "!empty")

        action: Action | None = reduce(lambda acc, cur: acc or actions.get(cur, None), identifiers, None)

        if set_match:
            self.action, self.action_data = action.action if action else None, action.action_data if action else None

        return action

    def get_absolute_path(self, root: Path | None = None) -> Path:
        """
        Get the absolute path of the file.

        Joins the root path and file's relative path or resolves the relative path if the root path is not provided.

        :param root: Optional root path to join with the relative path. If not provided, the file's root path is used.
        :return: The absolute path.
        """
        root = root or self.root
        return root.joinpath(self.relative_path) if root else self.relative_path.resolve()

    def get_checksum(self) -> str:
        """
        Get the checksum of the file.

        :return: The checksum of the file as a hex digest string.
        """
        self.checksum = file_checksum(self.get_absolute_path(self.root))
        return self.checksum

    def get_size(self) -> int:
        """
        Returns the size of the file.

        :return: The size of the file in bytes.
        """
        self.size = self.get_absolute_path(self.root).stat().st_size
        return self.size

    @property
    def name(self) -> str:
        """
        Get file name.

        :return: File name.
        """
        return self.relative_path.name

    @name.setter
    def name(self, new_name: str):
        self.relative_path = self.relative_path.with_name(new_name)

    @property
    def stem(self) -> str:
        """
        Get file stem.

        :return: File stem.
        """
        return self.relative_path.name

    @stem.setter
    def stem(self, new_stem: str):
        self.relative_path = self.relative_path.with_name(new_stem)

    @property
    def suffix(self) -> str:
        """
        Get file suffix.

        :return: File extension.
        """
        return self.relative_path.suffix.lower()

    @suffix.setter
    def suffix(self, new_suffix: str):
        self.relative_path = self.relative_path.with_suffix(new_suffix)
