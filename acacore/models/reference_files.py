"""Data models for the data on saved to different .json files on the `reference_files` repo."""
from typing import get_args as get_type_args
from typing import Literal

from pydantic import BaseModel
from pydantic import Field

TActionType = Literal[
    "convert",
    "extract",
    "replace",
    "manual",
    "rename",
    "ignore",
    "reidentify",
]
TReplaceTemplate = Literal[
    "text",
    "empty",
    "password-protected",
    "corrupted",
    "duplicate",
    "not-preservable",
    "not-convertable",
]

ActionTypeEnum: tuple[TActionType, ...] = get_type_args(TActionType)
ReplaceTemplateEnum: tuple[TReplaceTemplate, ...] = get_type_args(TReplaceTemplate)


class CustomSignature(BaseModel):
    bof: str | None = None
    eof: str | None = None
    operator: str | None = None
    puid: str | None = None
    signature: str | None = None
    extension: str | None = None


class ConvertAction(BaseModel):
    """
    Class representing an action to convert a file to a different format.

    :ivar converter: The converter to use for the conversion.
    :ivar outputs: The list of file types to convert to.
    """

    converter: str
    converter_type: Literal["master", "statutory", "access"]
    outputs: list[str] = Field(min_length=1)


class ExtractAction(BaseModel):
    """
    Class representing an action to extract data from a file.

    :ivar tool: The name of the tool used for extraction.
    :ivar extension: The suffix that the file should have. Defaults to None.
    :ivar dir_suffix: The output directory where the extracted data will be saved.
    """

    tool: str
    extension: str | None = None
    dir_suffix: str


class ReplaceAction(BaseModel):
    """
    Class representing a replacement action.

    :ivar template: The replacement template.
    :ivar template_text: Optional. Text to use instead of the default template, if template is set to "text".
    """

    template: TReplaceTemplate
    template_text: str | None = None


class ManualAction(BaseModel):
    """
    Class representing a manual action in a workflow.

    :ivar reason: The reason behind the manual action.
    :ivar process: The process for performing the manual action.
    """

    reason: str
    process: str


class IgnoreIfAction(BaseModel):
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


class IgnoreAction(BaseModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    :ivar reason: The reason for ignoring the file.
    :ivar ignore_if: An optional list of ignore conditions.
    """

    reason: str | None = None
    ignore_if: list[IgnoreIfAction] = Field(default_factory=list)


class ReIdentifyAction(BaseModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    :ivar reason: The reason for ignoring the file.
    """

    reason: str
    onfail: TActionType | None = None


class RenameAction(BaseModel):
    """
    Class representing an action to change file's extension.

    :ivar extension: A string representing the new extension for the file.
    """

    extension: str
    append: bool = False
    on_extension_mismatch: bool = False


class ActionData(BaseModel):
    """
    A class representing the data for a specific action.

    Separate from Action to avoid duplicating information in the File object.

    :ivar convert: A list of ConvertAction objects representing the conversion actions to be performed.
        Defaults to None.
    :ivar extract: An ExtractAction object representing the extraction action to be performed.
        Defaults to None.
    :ivar replace: A ReplaceAction object representing the replacement action to be performed.
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

    convert: list[ConvertAction | None] = None
    extract: ExtractAction | None = None
    replace: ReplaceAction | None = None
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
    action: TActionType
    ignore_warnings: list[str] = Field(default_factory=list, alias="ignore-warnings")

    @property
    def action_data(self) -> ActionData:
        """
        Return only the ActionData portion of the object.

        :return: The action data.
        """
        return ActionData(
            convert=self.convert,
            extract=self.extract,
            replace=self.replace,
            manual=self.manual,
            rename=self.rename,
            ignore=self.ignore,
            reidentify=self.reidentify,
        )
