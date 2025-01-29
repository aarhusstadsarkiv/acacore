from re import Match
from re import search
from typing import get_args as get_type_args
from typing import Literal
from typing import Self

from pydantic import AliasChoices
from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from .base import NoDefaultsModel

TActionType = Literal[
    "convert",
    "extract",
    "manual",
    "rename",
    "ignore",
    "reidentify",
]
TTemplateType = Literal[
    "text",
    "empty",
    "password-protected",
    "corrupted",
    "duplicate",
    "not-preservable",
    "not-convertable",
    "extracted-archive",
    "temporary-file",
]

ActionTypeEnum: tuple[TActionType, ...] = get_type_args(TActionType)
TemplateTypeEnum: tuple[TTemplateType, ...] = get_type_args(TTemplateType)


class CustomSignature(BaseModel):
    """
    Class representing a custom signature used for file identification.

    :param bof: The hexadecimal regex pattern representing the beginning of the file.
    :param eof: The hexadecimal regex pattern representing the end of the file.
    :param operator: The operator used for combining the begging and end of file patterns.
    :param puid: The PUID (PRONOM Unique Identifier) associated with the signature.
    :param signature: The long name of the signature.
    :param extension: The file extension associated with the signature.
    """

    puid: str
    signature: str
    bof: str | None = None
    eof: str | None = None
    operator: Literal["AND", "OR"] | None = None
    extension: str | None = None

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        if not self.bof and not self.eof:
            raise ValueError("One of bof or eof must be set.")
        if self.bof and self.eof and not self.operator:
            raise ValueError("Operator must be set if both bof and eof are set.")
        return self

    def match(self, bof: str | None, eof: str | None) -> int:
        if not bof and not eof:
            return 0
        elif self.bof and self.eof:
            match_bof: Match[str] | None = search(self.bof, bof or "")

            if self.operator == "AND" and not match_bof:
                return 0

            match_eof: Match[str] | None = search(self.eof, eof or "")

            if match_bof and match_eof:
                return (match_bof.end() - match_bof.start()) + (match_eof.end() - match_eof.start())
            elif self.operator == "OR" and match_bof:
                return match_bof.end() - match_bof.start()
            elif self.operator == "OR" and match_eof:
                return match_eof.end() - match_eof.start()
        elif self.bof and (match_bof := search(self.bof, bof or "")):
            return match_bof.end() - match_bof.start()
        elif self.eof and (match_eof := search(self.eof, eof or "")):
            return match_eof.end() - match_eof.start()

        return 0


class IgnoreIfAction(NoDefaultsModel):
    """
    Class representing conditions to ignore a file.

    The pixel counts and sizes are considered as the minimum allowed value.

    :ivar image_pixels_min: Minimum amount of pixels (width times height) for images.
    :ivar image_width_min: Minimum width (in pixels) for images.
    :ivar image_height_min: Minimum height (in pixels) for images.
    :ivar size: Minimum file size.
    """

    image_pixels_min: int | None = Field(None, gt=0)
    image_width_min: int | None = Field(None, gt=0)
    image_height_min: int | None = Field(None, gt=0)
    size: int | None = Field(None, gt=0)


class ReIdentifyAction(NoDefaultsModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    :ivar reason: The reason for ignoring the file.
    :ivar chunk_size: Specifies how many bytes should be used to search for custom signatures.
    :ivar on_fail: The action to take if the re-identification fails. Defaults to "null".
    """

    reason: str
    chunk_size: int | None = Field(None, ge=1)
    on_fail: Literal["action", "null"] = "null"


class ConvertAction(NoDefaultsModel):
    """
    Class representing an action to convert a file to a different format.

    :ivar tool: The converter to use for the conversion.
    :ivar output: The output target for the converter.
    """

    tool: str
    output: str | None = None

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        if not self.tool == "copy" and not self.output:
            raise ValueError("Missing output.")
        return self


class ExtractAction(NoDefaultsModel):
    """
    Class representing an action to extract data from a file.

    :ivar tool: The name of the tool used for extraction.
    :ivar extension: The suffix that the file should have. Defaults to None.
    """

    tool: str
    extension: str | None = None
    on_success: Literal["convert", "manual", "ignore"] | None = None


class ManualAction(NoDefaultsModel):
    """
    Class representing a manual action in a workflow.

    :ivar reason: The reason behind the manual action.
    :ivar process: The process for performing the manual action.
    """

    reason: str
    process: str


class IgnoreAction(NoDefaultsModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    If the template is set to ``text``, reason must be set to a non-empty string.

    :ivar template: The template type.
    :ivar reason: The reason for ignoring the file.
    """

    template: TTemplateType
    reason: str | None = None

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        if self.template == "text" and (self.reason is None or not self.reason.strip()):
            raise ValueError("Reason cannot be empty when template is set to text.")
        self.reason = (self.reason.strip() or None) if self.reason else None
        return self


class MasterConvertAction(NoDefaultsModel):
    """Class representing the instructions to convert a master file to access and statutory formats."""

    name: str | None = None
    description: str | None = None
    access: ConvertAction
    statutory: ConvertAction


class ActionData(NoDefaultsModel):
    """
    A class representing the data for a specific action.

    Separate from Action to avoid duplicating information in the File object.

    :ivar convert: A list of ConvertAction objects representing the conversion actions to be performed.
        Defaults to None.
    :ivar extract: An ExtractAction object representing the extraction action to be performed.
        Defaults to None.
    :ivar manual: A ManualAction object representing the manual action to be performed.
        Defaults to None.
    :ivar ignore: An IgnoreAction object representing the ignore action to be performed.
        Defaults to None.
    :ivar reidentify: A ReIdentifyAction object representing the re-identification action to be performed.
        Defaults to None.
    """

    reidentify: ReIdentifyAction | None = None
    convert: ConvertAction | None = None
    extract: ExtractAction | None = None
    manual: ManualAction | None = None
    ignore: IgnoreAction | None = None


class Action(ActionData):
    """
    Class representing an Action.

    Follows the format as outlined in the reference files repository. Subclasses ActionData to avoid duplicated
    properties in the File object.

    `JSON schema <https://github.com/aarhusstadsarkiv/reference-files/blob/main/fileformats.schema.json>`_

    :ivar name: The name of the action.
    :ivar description: The description of the action.
    :ivar action: The type of action.
    """

    name: str
    description: str | None = None
    alternatives: dict[str, str] = Field(default_factory=dict)
    action: TActionType | None
    ignore_warnings: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("ignore_warnings", "ignore-warnings"),
    )
    ignore_if: IgnoreIfAction | None = None

    # noinspection PyNestedDecorators
    @field_validator("alternatives", mode="before")
    @classmethod
    def _validate_alternatives(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            return {}
        elif not isinstance(value, dict):
            return value
        return {k.lower(): v for k, v in value.items()}

    @property
    def action_data(self) -> ActionData:
        """
        Return only the ActionData portion of the object.

        :return: The action data.
        """
        return ActionData.model_validate(self.model_dump())

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        if self.action is not None and getattr(self, self.action, None) is None:
            raise ValueError(f"missing {self.action!r}. If action is set, the action field must be set as well. ")
        return self
