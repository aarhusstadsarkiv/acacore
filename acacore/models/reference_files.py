"""Data models for the data on saved to different .json files on the `reference_files` repo."""
from typing import Optional

from pydantic import BaseModel
from pydantic import field_validator


class ReIdentifyModel(BaseModel):
    """Data model for the `to_re-identify` from reference_files."""

    puid: Optional[str] = None
    name: Optional[str] = None
    ext: Optional[str] = None
    reasoning: Optional[str] = None


class CustomSignature(BaseModel):
    """Data model for the `custom_signatures` from reference_files."""

    bof: Optional[str] = None
    eof: Optional[str] = None
    operator: Optional[str] = None
    puid: Optional[str] = None
    signature: Optional[str] = None
    extension: Optional[str] = None


class ConversionInstruction(BaseModel):
    puid: str
    converter: str
    outputs: list[str]


class ManualConversionInstruction(BaseModel):
    puid: str
    reasoning: str
    process: str


class ExtractionInstruction(BaseModel):
    puid: str
    tool: str
    dir_suffix: str


class IgnoreInstruction(BaseModel):
    puid: str
    description: str = ""
    extensions: list[str]
    reasoning: str

    # noinspection PyNestedDecorators
    @field_validator("extensions", mode="before")
    @classmethod
    def extensions_validator(cls, data: str):
        return list(filter(bool, map(str.strip, data.strip().split(","))))
