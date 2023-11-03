"""Data models for the data on saved to different .json files on the `reference_files` repo."""
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field

TActionType = Literal["convert", "extract", "manual", "rename", "ignore", "reidentify"]


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
    outputs: list[str] = Field(min_items=1)


class ExtractAction(BaseModel):
    """
    Class representing an action to extract data from a file.

    Attributes:
        tool (str): The name of the tool used for extraction.
        dir_suffix (str): The output directory where the extracted data will be saved.
    """

    tool: str
    dir_suffix: str


class ReplaceAction(BaseModel):
    """
    Class representing a replacement action.

    Attributes:
        template (str): The replacement template.
    """

    template: str


class ManualAction(BaseModel):
    """
    Class representing a manual action in a workflow.

    Attributes:
        reasoning (str): The reasoning behind the manual action.
        process (str): The process for performing the manual action.
    """

    reasoning: str
    process: str


class IgnoreAction(BaseModel):
    """
    Class representing an action to ignore a specific file based on the given reasoning.

    Attributes:
        reasoning (str): The reasoning for ignoring the file.
    """

    reasoning: str


class ReIdentifyAction(BaseModel):
    """
    Class representing an action to ignore a specific file based on the given reasoning.

    Attributes:
        reasoning (str): The reasoning for ignoring the file.
    """

    reasoning: str
    onfail: Optional[Literal["convert", "extract", "manual", "rename", "ignore"]] = None


class RenameAction(BaseModel):
    """
    Class representing an action to rename a file. It is a dictionary-based class with the following fields.

    Attributes:
        new_name (str): A string representing the new name for the file.
    """

    new_name: str


class ActionData(BaseModel):
    convert: Optional[list[ConvertAction]] = None
    extract: Optional[ExtractAction] = None
    manual: Optional[ManualAction] = None
    rename: Optional[RenameAction] = None
    ignore: Optional[IgnoreAction] = None
    reidentify: Optional[ReIdentifyAction] = None


class Action(ActionData):
    name: str
    action: TActionType
