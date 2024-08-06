"""Data models for the data on saved to different .json files on the `reference_files` repo."""

from re import match
from typing import get_args as get_type_args
from typing import Literal

from pydantic import AliasChoices
from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

from .base import NoDefaultsModel

TActionType = Literal[
    "convert",
    "extract",
    "template",
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

    bof: str | None = None
    eof: str | None = None
    operator: str | None = None
    puid: str | None = None
    signature: str | None = None
    extension: str | None = None


class ConvertAction(NoDefaultsModel):
    """
    Class representing an action to convert a file to a different format.

    :ivar converter: The converter to use for the conversion.
    :ivar outputs: The list of file types to convert to.
    """

    converter: str
    converter_type: Literal["master", "statutory", "access"]
    outputs: list[str] = Field(min_length=1)


class ExtractAction(NoDefaultsModel):
    """
    Class representing an action to extract data from a file.

    :ivar tool: The name of the tool used for extraction.
    :ivar extension: The suffix that the file should have. Defaults to None.
    :ivar dir_suffix: The output directory where the extracted data will be saved.
    """

    tool: str
    extension: str | None = None
    dir_suffix: str


class TemplateAction(NoDefaultsModel):
    """
    Class representing a template replacement action.

    :ivar template: The replacement template.
    :ivar template_text: Optional. Text to use instead of the default template, if template is set to "text".
    """

    template: TTemplateType
    template_text: str | None = None


class ManualAction(NoDefaultsModel):
    """
    Class representing a manual action in a workflow.

    :ivar reason: The reason behind the manual action.
    :ivar process: The process for performing the manual action.
    """

    reason: str
    process: str


class IgnoreIfAction(NoDefaultsModel):
    """
    Class representing conditions to ignore a file.

    The pixel counts and sizes are considered as the minimum allowed value.

    :ivar pixel_total: Total amount of pixels (width times height) for images.
    :ivar pixel_width: Width for images.
    :ivar pixel_height: Height for images.
    :ivar size: Size for all files.
    :ivar binary_size: Size for binary files.
    :ivar reason: A reason for the specific condition.
    """

    pixel_total: int | None = Field(None, gt=0)
    pixel_width: int | None = Field(None, gt=0)
    pixel_height: int | None = Field(None, gt=0)
    size: int | None = Field(None, gt=0)
    binary_size: int | None = Field(None, gt=0)
    reason: str | None = None


class IgnoreAction(NoDefaultsModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    :ivar reason: The reason for ignoring the file.
    :ivar ignore_if: An optional list of ignore conditions.
    """

    reason: str | None = None
    ignore_if: list[IgnoreIfAction] = Field(default_factory=list)


class ReIdentifyAction(NoDefaultsModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    :ivar reason: The reason for ignoring the file.
    """

    reason: str
    onfail: TActionType | None = None


class RenameAction(NoDefaultsModel):
    """
    Class representing an action to change file's extension.

    :ivar extension: A string representing the new extension for the file.
    """

    extension: str
    append: bool = False
    on_extension_mismatch: bool = False


class ActionData(NoDefaultsModel):
    """
    A class representing the data for a specific action.

    Separate from Action to avoid duplicating information in the File object.

    :ivar convert: A list of ConvertAction objects representing the conversion actions to be performed.
        Defaults to None.
    :ivar extract: An ExtractAction object representing the extraction action to be performed.
        Defaults to None.
    :ivar template: A TemplateAction object representing the template replacement action to be performed.
        Defaults to None.
    :ivar manual: A ManualAction object representing the manual action to be performed.
        Defaults to None.
    :ivar rename: A RenameAction object representing the renaming action to be performed.
        Defaults to None.
    :ivar ignore: An IgnoreAction object representing the ignore action to be performed.
        Defaults to None.
    :ivar reidentify: A ReIdentifyAction object representing the re-identification action to be performed.
        Defaults to None.
    """

    convert: list[ConvertAction] | None = None
    extract: ExtractAction | None = None
    # "replace" alias for template to support older versions of fileformats
    template: TemplateAction | None = Field(None, validation_alias=AliasChoices("template", "replace"))
    manual: ManualAction | None = None
    rename: RenameAction | None = None
    ignore: IgnoreAction | None = None
    reidentify: ReIdentifyAction | None = None


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
    action: TActionType
    ignore_warnings: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("ignore_warnings", "ignore-warnings"),
    )

    # noinspection PyNestedDecorators
    @field_validator("alternatives", mode="before")
    @classmethod
    def _validate_alternatives(cls, value: dict[str, str]) -> dict[str, str]:
        if not isinstance(value, dict):
            raise ValueError("Is not a dictionary.")
        if not all(isinstance(k, str) and match(r"^(\.[a-z0-9]+)+$", k) for k in value.keys()):
            raise ValueError("Keys are not valid extensions '(\\.[a-z0-9]+)+'.")
        if not all(isinstance(v, str) and match(r"^[a-zA-Z0-9_/-]+$", v) for v in value.values()):
            raise ValueError("Keys are not valid PUIDs '(\\.[a-z0-9]+)+'.")
        return {k.lower(): v for k, v in value.items()}

    @property
    def action_data(self) -> ActionData:
        """
        Return only the ActionData portion of the object.

        :return: The action data.
        """
        return ActionData.model_validate(super().model_dump())
