"""Data models for the data on saved to different .json files on the `reference_files` repo."""
from typing import get_args as get_type_args
from typing import Literal
from typing import Optional

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
    """Data model for the `custom_signatures` from reference_files."""

    bof: Optional[str] = None
    eof: Optional[str] = None
    operator: Optional[str] = None
    puid: Optional[str] = None
    signature: Optional[str] = None
    extension: Optional[str] = None


class ConvertAction(BaseModel):
    """
    Class representing an action to convert a file to a different format.

    Attributes:
        converter (str): The converter to use for the conversion.
        outputs (list[str]): The list of file types to convert to.
    """

    converter: str
    converter_type: Literal["master", "statutory", "access"]
    outputs: list[str] = Field(min_length=1)


class ExtractAction(BaseModel):
    """
    Class representing an action to extract data from a file.

    Attributes:
        tool (str): The name of the tool used for extraction.
        extension (Optional[str]): The suffix that the file should have. Defaults to None.
        dir_suffix (str): The output directory where the extracted data will be saved.
    """

    tool: str
    extension: Optional[str] = None
    dir_suffix: str


class ReplaceAction(BaseModel):
    """
    Class representing a replacement action.

    Attributes:
        template (str): The replacement template.
        template_text (Optional[str]): Optional. Text to use instead of the default template,
            if template is set to "text".
    """

    template: TReplaceTemplate
    template_text: Optional[str] = None


class ManualAction(BaseModel):
    """
    Class representing a manual action in a workflow.

    Attributes:
        reason (str): The reason behind the manual action.
        process (str): The process for performing the manual action.
    """

    reason: str
    process: str


class IgnoreIfAction(BaseModel):
    """
    Class representing conditions to ignore a file.

    The pixel counts and sizes are considered as the minimum allowed value.

    Attributes:
        pixel_total (Optional[int]): Total amount of pixels (width times height) for images.
        pixel_width (Optional[int]): Width for images.
        pixel_height (Optional[int]): Height for images.
        size (Optional[int]): Size for all files.
        binary_size (Optional[int]): Size for binary files.
        reason (Optional[int]): A reason for the specific condition.
    """

    pixel_total: Optional[int] = Field(None, gt=0)
    pixel_width: Optional[int] = Field(None, gt=0)
    pixel_height: Optional[int] = Field(None, gt=0)
    size: Optional[int] = Field(None, gt=0)
    binary_size: Optional[int] = Field(None, gt=0)
    reason: Optional[str] = None


class IgnoreAction(BaseModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    Attributes:
        reason (str): The reason for ignoring the file.
        ignore_if (list[IgnoreIfAction]): An optional list of ignore conditions.
    """

    reason: Optional[str] = None
    ignore_if: list[IgnoreIfAction] = Field(default_factory=list)


class ReIdentifyAction(BaseModel):
    """
    Class representing an action to ignore a specific file based on the given reason.

    Attributes:
        reason (str): The reason for ignoring the file.
    """

    reason: str
    onfail: Optional[TActionType] = None


class RenameAction(BaseModel):
    """
    Class representing an action to change file's extension.

    Attributes:
        extension (str): A string representing the new extension for the file.
    """

    extension: str
    append: bool = False
    on_extension_mismatch: bool = False


class ActionData(BaseModel):
    """
    A class representing the data for a specific action.

    Separate from Action to avoid duplicating information in the File object.

    Attributes:
        convert (Optional[list[ConvertAction]]): A list of ConvertAction objects representing the conversion
            actions to be performed. Defaults to None.
        extract (Optional[ExtractAction]): An ExtractAction object representing the extraction action to be
            performed. Defaults to None.
        replace (Optional[ReplaceAction]): A ReplaceAction object representing the replacement action to be
            performed. Defaults to None.
        manual (Optional[ManualAction]): A ManualAction object representing the manual action to be
            performed. Defaults to None.
        rename (Optional[RenameAction]): A RenameAction object representing the renaming action to be
            performed. Defaults to None.
        ignore (Optional[IgnoreAction]): An IgnoreAction object representing the ignore action to be
            performed. Defaults to None.
        reidentify (Optional[ReIdentifyAction]): A ReIdentifyAction object representing the re-identification
            action to be performed. Defaults to None.
    """

    convert: Optional[list[ConvertAction]] = None
    extract: Optional[ExtractAction] = None
    replace: Optional[ReplaceAction] = None
    manual: Optional[ManualAction] = None
    rename: Optional[RenameAction] = None
    ignore: Optional[IgnoreAction] = None
    reidentify: Optional[ReIdentifyAction] = None


class Action(ActionData):
    """
    Class representing an Action.

    Follows the format as outlined in the reference files repository.
    Subclasses ActionData to avoid duplicated properties in the File object.

    See Also:
        https://github.com/aarhusstadsarkiv/reference-files/blob/main/fileformats.schema.json

    Attributes:
        name (str): The name of the action.
        description (Optional[str]): The description of the action.
        action (Optional[TActionType]): The type of action.
    """

    name: str
    description: Optional[str] = None
    action: TActionType

    @property
    def action_data(self) -> ActionData:
        """
        Return only the ActionData portion of the object.

        Returns:
            ActionData: The action data.
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
