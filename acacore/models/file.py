from functools import reduce
from os import PathLike
from pathlib import Path
from typing import Self
from uuid import UUID
from uuid import uuid4

from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
from pydantic import UUID4

from acacore.siegfried.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.siegfried.siegfried import TSiegfriedFileClass
from acacore.utils.functions import file_checksum
from acacore.utils.functions import get_bof
from acacore.utils.functions import get_eof
from acacore.utils.functions import image_size
from acacore.utils.functions import is_binary
from acacore.utils.functions import is_valid_suffix

from .reference_files import Action
from .reference_files import ActionData
from .reference_files import CustomSignature
from .reference_files import IgnoreAction
from .reference_files import IgnoreIfAction
from .reference_files import ManualAction
from .reference_files import TActionType


def ignore_if(file: "OriginalFile", rules: IgnoreIfAction) -> "OriginalFile":
    action: TActionType | None = None
    ignore_action: IgnoreAction = IgnoreAction(template="not-preservable")

    if rules.image_pixels_min or rules.image_width_min or rules.image_height_min:
        width, height = image_size(file.get_absolute_path())
        if rules.image_width_min and width < rules.image_width_min:
            action = "ignore"
            ignore_action.reason = f"Image width is too small ({width}px < {rules.image_width_min})"
        elif rules.image_height_min and height < rules.image_height_min:
            action = "ignore"
            ignore_action.reason = f"Image height is too small  ({height}px < {rules.image_height_min})"
        elif rules.image_pixels_min and (width * height) < rules.image_pixels_min:
            action = "ignore"
            ignore_action.reason = f"Image resolution is too small  ({width * height}px < {rules.image_pixels_min})"
    elif rules.size and file.size < rules.size:
        action = "ignore"
        ignore_action.reason = "File size is too small"

    if action:
        file.action = action
        file.action_data = file.action_data or ActionData()
        file.action_data.ignore = ignore_action

    return file


class BaseFile(BaseModel):
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
    :ivar root: The root directory for the file.
    """

    uuid: UUID4 = Field(default_factory=uuid4)
    checksum: str
    relative_path: Path
    is_binary: bool
    size: int
    puid: str | None
    signature: str | None
    warning: list[str] | None = None
    root: Path | None = None

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        root: str | PathLike[str],
        siegfried: Siegfried | SiegfriedFile = None,
        custom_signatures: list[CustomSignature] | None = None,
        uuid: UUID | None = None,
    ) -> Self:
        path = Path(path)
        root = Path(root)
        file = cls(
            root=root,
            relative_path=path.relative_to(root) if root else path,
            uuid=uuid or uuid4(),
            checksum=file_checksum(path),
            is_binary=is_binary(path),
            size=path.stat().st_size,
            puid=None,
            signature=None,
            warning=None,
        )

        if siegfried:
            file.identify(siegfried, set_match=True)

        if custom_signatures and not file.puid:
            file.identify_custom(custom_signatures, set_match=True)

        return file

    def identify(self, sf: Siegfried | SiegfriedFile, *, set_match: bool = False) -> SiegfriedFile:
        """
        Identify the file using `siegfried`.

        :param sf: A ``Siegfried`` or ``SiegfriedFile`` object.
        :param set_match: Set results of Siegfried match if ``True``, defaults to ``False``.
        :return: The ``SiegfriedFile`` result.
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
        chunk_size: int | None = 1024,
        set_match: bool = False,
    ) -> CustomSignature | None:
        """
        Uses the BOF and EOF to try to determine a PUID for the file.

        :param custom_signatures: A list of ``CustomSignature`` that the file should be checked against.
        :param chunk_size: Optionally, the chunk size to use to search for custom signatures. Defaults to 1024.
        :param set_match: Set results of match if ``True``, defaults to ``False``.
        :return: The matched ``CustomSignature`` object, if any, otherwise ``None``.
        """
        bof = get_bof(self.get_absolute_path(self.root), chunk_size or 1024).hex()
        eof = get_eof(self.get_absolute_path(self.root), chunk_size or 1024).hex()
        signature: CustomSignature | None = None
        signature_length: int = 0

        for sig in custom_signatures:
            if (match_length := sig.match(bof, eof)) > signature_length:
                signature = sig
                signature_length = match_length

        if set_match and signature:
            self.puid = signature.puid
            self.signature = signature.signature
            if signature.extension and self.suffix != signature.extension:
                self.warning = ["extension mismatch"]
            else:
                self.warning = None
        elif set_match:
            self.puid = self.signature = self.warning = None

        return signature

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
        return self.relative_path.stem

    @stem.setter
    def stem(self, new_stem: str):
        self.relative_path = self.relative_path.with_stem(new_stem)

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

    @property
    def suffixes(self) -> str:
        """
        Get file suffixes. Excludes invalid ones.

        :return: All the file extensions as a string.
        """
        suffixes: list[str] = []
        for suffix in self.relative_path.suffixes[::-1]:
            if is_valid_suffix(suffix):
                suffixes.insert(0, suffix)
            else:
                break
        return "".join(suffixes)

    @suffixes.setter
    def suffixes(self, new_suffixes: str):
        self.relative_path = self.relative_path.with_name(self.name.removesuffix(self.suffixes) + new_suffixes)


class OriginalFile(BaseFile):
    """
    File model containing all information used by the rest of the archival suite of tools.

    :ivar action: The name of the main action for the file's PUID, if one exists.
    :ivar action_data: The data for the action for the file's PUID, if one exists.
    :ivar processed: True if the file has been processed, false otherwise.
    :ivar lock: True if the file is locked for edits, false otherwise.
    :ivar original_name: The original name of the file.
    """

    action: TActionType | None = None
    action_data: ActionData = Field(default_factory=ActionData)
    parent: UUID4 | None = None
    processed: bool = False
    lock: bool = False
    original_name: str

    # noinspection PyNestedDecorators
    @model_validator(mode="before")
    @classmethod
    def _model_validator(cls, data: dict):
        if isinstance(data, dict):
            data["original_name"] = data.get("original_name", "").strip() or Path(data["relative_path"]).name
        return data

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        root: str | PathLike[str],
        siegfried: Siegfried | SiegfriedFile | None = None,
        actions: dict[str, Action] | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        uuid: UUID | None = None,
        parent: UUID4 | None = None,
        processed: bool = False,
        lock: bool = False,
    ) -> Self:
        file_base = super().from_file(path, root, None, uuid=uuid)
        file = cls(
            uuid=file_base.uuid,
            root=file_base.root,
            relative_path=file_base.relative_path,
            checksum=file_base.checksum,
            is_binary=file_base.is_binary,
            size=file_base.size,
            puid=None,
            signature=None,
            warning=None,
            parent=parent,
            processed=processed or False,
            lock=lock or False,
        )

        from_custom_signatures: bool = False
        file_classes: list[TSiegfriedFileClass] = []
        action: Action | None = None

        if siegfried:
            siegfried_match = file.identify(siegfried, set_match=True).best_match()
            file_classes = siegfried_match.match_class if siegfried_match else []

        if custom_signatures and not file.puid:
            file.identify_custom(custom_signatures, set_match=True)
            from_custom_signatures = True

        if actions:
            action = file.get_action(actions, file_classes)

        if action:
            if action.reidentify and custom_signatures and not from_custom_signatures:
                if file.identify_custom(custom_signatures, chunk_size=action.reidentify.chunk_size, set_match=True):
                    if new_action := file.get_action(actions, file_classes):
                        action = new_action
                    else:
                        action = Action(
                            name="",
                            action="manual",
                            manual=ManualAction(reason="No action available for custom PUID", process=""),
                        )
                elif action.reidentify.on_fail == "action":
                    pass
                elif action.reidentify.on_fail == "null":
                    action.action = None

            file.action = action.action
            file.action_data = action.action_data

            if action.ignore_if:
                file = ignore_if(file, action.ignore_if)

            if file.action != "ignore" and actions and "*" in actions and actions["*"].ignore_if:
                file = ignore_if(file, actions["*"].ignore_if)

            if action.ignore_warnings and file.warning is not None:
                ignore_warnings: list[str] = [iw.lower() for iw in action.ignore_warnings]
                file.warning = [w for w in file.warning if w.lower() not in ignore_warnings]
                file.warning = file.warning or None

        return file

    def get_action(
        self,
        actions: dict[str, Action],
        file_classes: list[TSiegfriedFileClass] | None = None,
        *,
        set_match: bool = False,
    ) -> Action | None:
        """
        Returns the ``Action`` matching the file's PUID.

        :param actions: A dictionary containing the available actions.
        :param file_classes: A list of file classes or ``None``.
        :param set_match: Set the matched action if ``True``, defaults to ``False``.
        :return: The matched ``Action`` object, if any, otherwise ``None``.
        """
        identifiers: list[str] = [
            f"!name={self.relative_path.name}",
            f"!iname={self.relative_path.name.lower()}",
        ]

        if not self.size:
            identifiers.insert(0, "!empty")
        if self.puid:
            identifiers.append(self.puid)
        if self.suffix:
            identifiers.append(f"!ext={''.join(self.relative_path.suffixes)}")
        if file_classes:
            identifiers.extend(f"!{c}" for c in file_classes)
        if self.is_binary:
            identifiers.append("!binary")

        action: Action | None = reduce(lambda acc, cur: acc or actions.get(cur), identifiers, None)

        if action and action.alternatives and (new_puid := action.alternatives.get(self.suffixes.lower(), None)):
            puid: str | None = self.puid
            self.puid = new_puid
            if new_action := self.get_action(actions, file_classes):
                action = new_action
            else:
                self.puid = puid

        if set_match and action:
            self.signature = action.name
            self.action = action.action
            self.action_data = action.action_data
        elif set_match:
            self.signature = self.signature if self.puid else None
            self.action = None
            self.action_data = ActionData()

        return action


class MasterFile(BaseFile):
    original_uuid: UUID4 | None = None


class ConvertedFile(BaseFile):
    original_uuid: UUID4 | None = None
