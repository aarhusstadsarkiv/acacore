from functools import reduce
from math import ceil
from os import PathLike
from pathlib import Path
from typing import Literal
from typing import Self
from uuid import UUID
from uuid import uuid4

from chardet import ResultDict
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
from .reference_files import ConvertAction
from .reference_files import CustomSignature
from .reference_files import IgnoreAction
from .reference_files import IgnoreIfAction
from .reference_files import ManualAction
from .reference_files import MasterConvertAction
from .reference_files import TActionType


def ignore_if(file: "OriginalFile", rules: IgnoreIfAction) -> tuple[TActionType | None, ActionData]:
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
        action_data = file.action_data.model_copy(deep=True)
        action_data.ignore = ignore_action
        return action, action_data

    return file.action, file.action_data


def get_identifier[A](file: "BaseFile", file_classes: list[TSiegfriedFileClass], actions: dict[str, A]) -> A | None:
    identifiers: list[str] = [
        f"!name={file.relative_path.name}",
        f"!iname={file.relative_path.name.lower()}",
    ]

    if not file.size:
        identifiers.insert(0, "!empty")
    if file.puid:
        identifiers.append(file.puid)
    if file.suffix:
        identifiers.append(f"!ext={''.join(file.relative_path.suffixes)}")
    if file_classes:
        identifiers.extend(f"!{c}" for c in file_classes)
    if file.is_binary:
        identifiers.append("!binary")

    return reduce(lambda acc, cur: acc or actions.get(cur), identifiers, None)


class BaseFile(BaseModel):
    """
    Base model for file identification.

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
    encoding: ResultDict | None
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
        siegfried: Siegfried | SiegfriedFile | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        uuid: UUID | None = None,
        *,
        encoding: bool | str | None = None,
    ) -> Self:
        """
        Create a file object from a given path.

        :param path: The path to the file.
        :param root: The folder to use as root for the file.
        :param siegfried: Optionally, an instance of ``Siegfried`` or ``SiegfriedFile`` to identify the file with.
        :param custom_signatures: Optionally, a list of ``CustomSignature`` to identify the file if ``siegfried`` is
            not provided or fails to find a match.
        :param uuid: Optionally, the UUID of the file.
        :param encoding: ``True`` to calculate encoding regardless of ``is_binary`` value, ``False`` to never calculate
            encoding, a ``str`` to set the encoding manually, or ``None`` to calculate it automatically when the file
            is detected as binary.
        :return: A ``BaseFile`` object.
        """
        path = Path(path)
        root = Path(root)
        file = BaseFile(
            root=root,
            relative_path=path.relative_to(root) if root else path,
            uuid=uuid or uuid4(),
            checksum="",
            encoding=None,
            is_binary=is_binary(path),
            size=path.stat().st_size,
            puid=None,
            signature=None,
            warning=None,
        )
        file.checksum, file.encoding = file_checksum(
            path,
            encoding=encoding if isinstance(encoding, bool | str) else not file.is_binary,
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
        self.checksum = file_checksum(self.get_absolute_path(self.root), False)[0]
        return self.checksum

    def get_encoding(self) -> ResultDict | None:
        self.encoding = file_checksum(self.get_absolute_path(self.root), True)[1]
        return self.encoding

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
        return self.relative_path.name.removesuffix(self.suffixes)

    @stem.setter
    def stem(self, new_stem: str):
        self.relative_path = self.relative_path.with_name(new_stem).with_suffix(self.suffixes)

    @property
    def suffix(self) -> str:
        """
        Get file suffix.

        :return: File extension.
        """
        return self.relative_path.suffix

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
    File model for OriginalDocuments files.

    :ivar action: The name of the main action for the file's PUID, if one exists.
    :ivar action_data: The data for the action for the file's PUID, if one exists.
    :ivar processed: True if the file has been processed, false otherwise.
    :ivar lock: True if the file is locked for edits, false otherwise.
    :ivar original_path: The original relative path of the file.
    """

    action: TActionType | None = None
    action_data: ActionData = Field(default_factory=ActionData)
    parent: UUID4 | None = None
    processed: bool = False
    lock: bool = False
    original_path: Path

    # noinspection PyNestedDecorators
    @model_validator(mode="before")
    @classmethod
    def _model_validator(cls, data: dict):
        if isinstance(data, dict):
            data["original_path"] = data.get("original_path", "") or data["relative_path"]
        return data

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        root: str | PathLike[str],
        siegfried: Siegfried | SiegfriedFile | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        actions: dict[str, Action] | None = None,
        uuid: UUID | None = None,
        parent: UUID | None = None,
        processed: bool = False,
        lock: bool = False,
        *,
        encoding: bool | str | None = None,
    ) -> Self:
        """
        Create a file object from a given path.

        :param path: The path to the file.
        :param root: The folder to use as root for the file.
        :param siegfried: Optionally, an insteance of ``Siegfried`` or ``SiegfriedFile`` to identify the file with.
        :param custom_signatures: Optionally, a list of ``CustomSignature`` to identify the file if ``siegfried`` is
            not provided or fails to find a match.
        :param actions: Optionally, a dictionary of ``Action`` objects to assign an action to the file.
        :param uuid: Optionally, the UUID of the file.
        :param parent: Optional, the UUID of the parent file.
        :param processed: Optionally, a boolean indicating if the file was processed.
        :param lock: Optionally, a boolean indicating if the file was processed.
        :param encoding: ``True`` to calculate encoding regardless of ``is_binary`` value, ``False`` to never calculate
            encoding, a ``str`` to set the encoding manually, or ``None`` to calculate it automatically when the file
            is detected as binary.
        :return: An ``OriginalFile`` object.
        """
        file_base = super().from_file(path, root, None, uuid=uuid, encoding=encoding)
        file = OriginalFile(
            uuid=file_base.uuid,
            root=file_base.root,
            relative_path=file_base.relative_path,
            checksum=file_base.checksum,
            encoding=file_base.encoding,
            is_binary=file_base.is_binary,
            size=file_base.size,
            puid=None,
            signature=None,
            warning=None,
            parent=parent,
            processed=processed or False,
            lock=lock or False,
            original_path=file_base.relative_path,
        )

        file.identify(siegfried, custom_signatures, actions)

        return file

    def identify(
        self,
        siegfried: Siegfried | SiegfriedFile | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        actions: dict[str, Action] | None = None,
    ):
        """
        Identify the file using `siegfried` or custom signatures and assign actions.

        If neither ``siegfried`` nor ``custom_signatures`` are provided, then no changes are made.

        If ``actions`` is provided and no action can be found with the new identity, the file's action fields are reset.

        :param siegfried: A ``Siegfried`` or ``SiegfriedFile`` object.
        :param custom_signatures: A list of ``CustomSignature`` objects.
        :param actions: A dictionary containing the available actions as ``Action`` objects.
        """
        if not siegfried and not custom_signatures:
            return

        from_custom_signatures: bool = False
        file_classes: list[TSiegfriedFileClass] = []
        action: Action | None = None

        if siegfried:
            siegfried_match = super().identify(siegfried, set_match=True).best_match()
            file_classes = siegfried_match.match_class if siegfried_match else []

        if custom_signatures and not self.puid:
            self.identify_custom(custom_signatures, set_match=True)
            from_custom_signatures = True

        if actions:
            action = self.get_action(actions, file_classes)

        if action:
            if action.reidentify and custom_signatures and not from_custom_signatures:
                if self.identify_custom(
                    custom_signatures,
                    chunk_size=action.reidentify.chunk_size,
                    set_match=True,
                ):
                    if new_action := self.get_action(actions, file_classes):
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

            self.action = action.action
            self.action_data = action.action_data

            if action.ignore_if:
                self.action, self.action_data = ignore_if(self, action.ignore_if)

            if self.action != "ignore" and actions and "*" in actions and actions["*"].ignore_if:
                self.action, self.action_data = ignore_if(self, actions["*"].ignore_if)

            if action.ignore_warnings and self.warning is not None:
                ignore_warnings: list[str] = [iw.lower() for iw in action.ignore_warnings]
                self.warning = [w for w in self.warning if w.lower() not in ignore_warnings]
                self.warning = self.warning or None
        elif actions:
            self.action = None
            self.action_data = ActionData()

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
        action: Action | None = get_identifier(self, file_classes, actions)
        from_alternative: bool = False

        if action and action.alternatives and (new_puid := action.alternatives.get(self.suffixes.lower(), None)):
            puid: str | None = self.puid
            self.puid = new_puid
            if new_action := self.get_action(actions, file_classes):
                action = new_action
                from_alternative = True
            else:
                self.puid = puid

        if set_match and action:
            self.signature = action.name
            self.action = action.action
            self.action_data = action.action_data
            if from_alternative and self.warning:
                self.warning = [w for w in self.warning if w.lower() != "extension mismatch"]
        elif set_match:
            self.signature = self.signature if self.puid else None
            self.action = None
            self.action_data = ActionData()

        return action


class ConvertedFile(BaseFile):
    """
    File model for output of file conversion.

    :ivar original_uuid: UUID of the parent file that was converted.
    """

    original_uuid: UUID4 | None = None

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        root: str | PathLike[str],
        original_uuid: UUID | None = None,
        siegfried: Siegfried | SiegfriedFile | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        uuid: UUID | None = None,
        *,
        encoding: bool | str | None = None,
    ) -> Self:
        """
        Create a file object from a given path.

        :param path: The path to the file.
        :param root: The folder to use as root for the file.
        :param original_uuid: The UUID of the parent file.
        :param siegfried: Optionally, an insteance of ``Siegfried`` or ``SiegfriedFile`` to identify the file with.
        :param custom_signatures: Optionally, a list of ``CustomSignature`` to identify the file if ``siegfried`` is
            not provided or fails to find a match.
        :param uuid: Optionally, the UUID of the file.
        :param encoding: ``True`` to calculate encoding regardless of ``is_binary`` value, ``False`` to never calculate
            encoding, a ``str`` to set the encoding manually, or ``None`` to calculate it automatically when the file
            is detected as binary.
        :return: A ``ConvertedFile`` object.
        """
        file_base = super().from_file(path, root, siegfried, custom_signatures, uuid, encoding=encoding)
        return ConvertedFile(
            uuid=file_base.uuid,
            checksum=file_base.checksum,
            encoding=file_base.encoding,
            relative_path=file_base.relative_path,
            root=file_base.root,
            is_binary=file_base.is_binary,
            size=file_base.size,
            puid=file_base.puid,
            signature=file_base.signature,
            warning=file_base.warning,
            original_uuid=original_uuid,
        )


class MasterFile(ConvertedFile):
    """
    File model for MasterDocuments files converted from OriginalDocuments.

    :ivar convert_access: Convert action to generate access file.
    :ivar convert_statutory: Convert action to generate statutory file.
    :ivar processed: Processed status as a bit flag: 0 none, 1 access, 2 statutory.
    """

    convert_access: ConvertAction | None = None
    convert_statutory: ConvertAction | None = None
    processed: int = Field(0, ge=0, le=3)

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        root: str | PathLike[str],
        original_uuid: UUID | None = None,
        siegfried: Siegfried | SiegfriedFile | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        actions: dict[str, MasterConvertAction] | None = None,
        uuid: UUID | None = None,
        processed: int = 0,
        *,
        encoding: bool | str | None = None,
    ) -> Self:
        """
        Create a file object from a given path.

        :param path: The path to the file.
        :param root: The folder to use as root for the file.
        :param original_uuid: The UUID of the parent file.
        :param siegfried: Optionally, an insteance of ``Siegfried`` or ``SiegfriedFile`` to identify the file with.
        :param custom_signatures: Optionally, a list of ``CustomSignature`` to identify the file if ``siegfried`` is
            not provided or fails to find a match.
        :param actions: Optionally, a dictionary of ``MasterConvertAction`` objects to assign an action to the file.
        :param uuid: Optionally, the UUID of the file.
        :param processed: Optionally, a bit flag indicating if the file was processed: 0 none, 1 access, 2 statutory.
        :param encoding: ``True`` to calculate encoding regardless of ``is_binary`` value, ``False`` to never calculate
            encoding, a ``str`` to set the encoding manually, or ``None`` to calculate it automatically when the file
            is detected as binary.
        :return: A ``MasterFile`` object.
        """
        file_base = super().from_file(path, root, original_uuid, siegfried, custom_signatures, uuid, encoding=encoding)
        file = MasterFile(
            uuid=file_base.uuid,
            checksum=file_base.checksum,
            encoding=file_base.encoding,
            relative_path=file_base.relative_path,
            root=file_base.root,
            is_binary=file_base.is_binary,
            size=file_base.size,
            puid=file_base.puid,
            signature=file_base.signature,
            warning=file_base.warning,
            original_uuid=file_base.original_uuid,
            processed=processed,
        )

        file.identify(siegfried, custom_signatures, actions)

        return file

    def identify(
        self,
        siegfried: Siegfried | SiegfriedFile | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        actions: dict[str, MasterConvertAction] | None = None,
    ):
        """
        Identify the file using `siegfried` or custom signatures and assign actions.

        If neither ``siegfried`` nor ``custom_signatures`` are provided, then no changes are made.

        If ``actions`` is provided and no action can be found with the new identity, the file's action fields are reset.

        :param siegfried: A ``Siegfried`` or ``SiegfriedFile`` object.
        :param custom_signatures: A list of ``CustomSignature`` objects.
        :param actions: A dictionary containing the available actions as ``MasterConvertAction`` objects.
        """
        if not siegfried and not custom_signatures:
            return

        file_classes: list[TSiegfriedFileClass] = []

        if siegfried and (match := super().identify(siegfried, set_match=True).best_match()):
            file_classes = match.match_class
        elif custom_signatures:
            self.identify_custom(custom_signatures, set_match=True)

        if actions:
            self.get_action("access", actions, file_classes, set_match=True)
            self.get_action("statutory", actions, file_classes, set_match=True)

    def get_action(
        self,
        target: Literal["access", "statutory", "all"],
        actions: dict[str, MasterConvertAction],
        file_classes: list[TSiegfriedFileClass] | None = None,
        *,
        set_match: bool = False,
    ) -> MasterConvertAction | None:
        """
        Returns the access ``Action`` matching the file's PUID.

        :param target: Which action to set, use "all" for both.
        :param actions: A dictionary containing the available access actions.
        :param file_classes: A list of file classes or ``None``.
        :param set_match: Set the matched action if ``True``, defaults to ``False``.
        :return: The matched ``Action`` object, if any, otherwise ``None``.
        """
        action: MasterConvertAction | None = get_identifier(self, file_classes, actions)

        if set_match and action:
            if target in ("access", "all"):
                self.convert_access = action.access
            if target in ("statutory", "all"):
                self.convert_statutory = action.statutory
        elif set_match:
            if target in ("access", "all"):
                self.convert_access = None
            if target in ("statutory", "all"):
                self.convert_statutory = None

        return action


class StatutoryFile(ConvertedFile):
    """
    File model for MasterDocuments files converted from OriginalDocuments.

    :ivar doc_collection: docCollection number.
    :ivar doc_id: Document ID.
    """

    doc_collection: int | None = Field(ge=1)
    doc_id: int | None = Field(ge=1)

    @model_validator(mode="after")
    def _model_validator(self) -> Self:
        if self.doc_collection is None and self.doc_id is not None:
            raise ValueError("doc_collection must be specified when doc_id is specified")
        if self.doc_collection is not None and self.doc_id is None:
            raise ValueError("doc_id must be specified when doc_collection is specified")
        return self

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        root: str | PathLike[str],
        original_uuid: UUID | None = None,
        siegfried: Siegfried | SiegfriedFile | None = None,
        custom_signatures: list[CustomSignature] | None = None,
        uuid: UUID | None = None,
        doc_collection: int | None = None,
        doc_id: int | None = None,
        *,
        encoding: bool | str | None = None,
    ) -> Self:
        """
        Create a file object from a given path.

        :param path: The path to the file.
        :param root: The folder to use as root for the file.
        :param original_uuid: The UUID of the parent file.
        :param siegfried: Optionally, an insteance of ``Siegfried`` or ``SiegfriedFile`` to identify the file with.
        :param custom_signatures: Optionally, a list of ``CustomSignature`` to identify the file if ``siegfried`` is
            not provided or fails to find a match.
        :param uuid: Optionally, the UUID of the file.
        :param doc_collection: Optionally, the docCollection number.
        :param doc_id: Optionally, the document ID.
        :param encoding: ``True`` to calculate encoding regardless of ``is_binary`` value, ``False`` to never calculate
            encoding, a ``str`` to set the encoding manually, or ``None`` to calculate it automatically when the file
            is detected as binary.
        :return: A ``StatutoryFile`` object.
        """
        file_base = super().from_file(path, root, original_uuid, siegfried, custom_signatures, uuid, encoding=encoding)

        return StatutoryFile(
            uuid=file_base.uuid,
            checksum=file_base.checksum,
            encoding=file_base.encoding,
            relative_path=file_base.relative_path,
            root=file_base.root,
            is_binary=file_base.is_binary,
            size=file_base.size,
            puid=file_base.puid,
            signature=file_base.signature,
            warning=file_base.warning,
            original_uuid=file_base.original_uuid,
            doc_collection=doc_collection,
            doc_id=doc_id,
        )

    def set_doc_id(self, doc_id: int, docs_in_collection: int):
        """
        Set the docCollection number and document ID.

        :param doc_id: The document ID.
        :param docs_in_collection: The number of documents in each collection.
        """
        assert doc_id >= 1
        assert docs_in_collection >= 1
        self.doc_collection = ceil(docs_in_collection / doc_id)
        self.doc_id = doc_id
