"""Data models for the data on saved to different .json files on the `reference_files` repo."""
from typing import Optional

from pydantic import BaseModel


class ReIdentifyModel(BaseModel):
    """Data model for the `to_reidentify` from reference_files."""

    puid: Optional[str] = None
    name: Optional[str] = None
    ext: Optional[str] = None
    reasoning: Optional[str] = None


class CustomSignature(BaseModel):
    """Data model for the `costum_signatures` from reference_files."""

    bof: Optional[str] = None
    eof: Optional[str] = None
    operator: Optional[str] = None
    puid: Optional[str] = None
    signature: Optional[str] = None
    extension: Optional[str] = None
