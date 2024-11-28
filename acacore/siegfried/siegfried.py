from datetime import datetime
from os import PathLike
from pathlib import Path
from re import compile as re_compile
from subprocess import CompletedProcess
from subprocess import run
from typing import get_args as get_type_args
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
from pydantic.networks import AnyUrl
from pydantic.networks import HttpUrl

from acacore.exceptions.files import IdentificationError

_byte_match_regexp_single = re_compile(r"^byte match at (\d+), *(\d+)( *\([^)]*\))?$")
_byte_match_regexp_multi = re_compile(r"^byte match at \[\[(\d+) +(\d+)]( \[\d+ +\d+])*]( \([^)]*\))?$")
_extension_match = re_compile(r"^extension match (.+)$")
TSignaturesProvider = Literal["pronom", "loc", "tika", "freedesktop", "pronom-tika-loc", "deluxe", "archivematica"]
TSiegfriedFileClass = Literal[
    "aggregate",
    "audio",
    "database",
    "dataset",
    "email",
    "font",
    "gis",
    "image (raster)",
    "image (vector)",
    "model",
    "page description",
    "presentation",
    "spreadsheet",
    "text (mark-up)",
    "text (structured)",
    "text (unstructured)",
    "video",
    "word processor",
]


def _check_process(process: CompletedProcess) -> CompletedProcess:
    """
    Check process and raise exception if it failed.

    :raises IdentificationError: If the process ends with a return code other than 0.
    """
    if process.returncode != 0:
        raise IdentificationError(process.stderr or process.stdout or f"Unknown error code {process.returncode}")

    return process


class SiegfriedIdentifier(BaseModel):
    """
    A class representing an identifiers used by the Siegfried identification tool.

    :ivar name: The name of the Siegfried identifier.
    :ivar details: Additional details or description of the identifier.
    """

    name: str
    details: str


class SiegfriedMatch(BaseModel):
    """
    A class representing a match generated by the Siegfried identification tool.

    :ivar ns: The namespace of the match.
    :ivar id: The identifier of the match.
    :ivar format: The format of the match.
    :ivar version: The version of the match.
    :ivar mime: The MIME type of the match.
    :ivar match_class: The class of the match.
    :ivar basis: The basis of the match.
    :ivar warning: The warning messages of the match.
    :ivar URI: The URI of the match.
    :ivar permalink: The permalink of the match.
    """

    ns: str
    id: str | None
    format: str
    version: str | None = None
    mime: str
    match_class: list[TSiegfriedFileClass] = Field(default_factory=list, alias="class")
    basis: list[str]
    warning: list[str]
    URI: AnyUrl | None = None
    permalink: HttpUrl | None = None

    def byte_match(self) -> int | None:
        """
        Get the length of the byte match, if any, or None.

        :return: The length of the byte match or None, if the match was not on the basis of bytes.
        """
        for basis in self.basis:
            match = _byte_match_regexp_single.match(basis) or _byte_match_regexp_multi.match(basis)
            if match:
                return (int(match.group(2)) - int(match.group(1))) if match else None
        return None

    def extension_match(self) -> str | None:
        """
        Get the matched extension.

        :return: The matched extension or None, if the match was not on the basis of the extension.
        """
        for basis in self.basis:
            match = _extension_match.match(basis)
            if match:
                return match.group(1) if match else None
        return None

    def extension_mismatch(self) -> bool:
        """
        Check whether the match has an extension mismatch warning.

        :return: True if the match has an extension mismatch warning, False otherwise
        """
        return "extension mismatch" in self.warning

    def filename_mismatch(self) -> bool:
        """
        Check whether the match has a filename mismatch warning.

        :return: True if the match has a filename mismatch warning, False otherwise
        """
        return "filename mismatch" in self.warning

    def sort_tuple(self) -> tuple[int, int, int, int, int]:
        """
        Get a tuple of integers useful for sorting matches.

        The fields used for the tuple are: byte match, extension match, format, version, and mime.

        :return: A tuple of 5 integers.
        """
        return (
            self.byte_match() or 0,
            1 if self.extension_match() else 0,
            1 if self.format else 0,
            1 if self.version else 0,
            1 if self.mime else 0,
        )

    # noinspection PyNestedDecorators
    @model_validator(mode="before")
    @classmethod
    def _unknown_id(cls, data: object):
        if isinstance(data, dict):
            return {
                **data,
                "id": None if data["id"].lower().strip() == "unknown" else data["id"].strip() or None,
                "basis": filter(bool, map(str.strip, data["basis"].strip().split(";"))),
                "warning": filter(bool, map(str.strip, data["warning"].strip().split(";"))),
                "class": [c for c in map(str.strip, data.get("class", "").lower().split(",")) if c],
            }
        return data


class SiegfriedFile(BaseModel):
    """
    The SiegfriedFile class represents a file that has been analyzed by Siegfried.

    It contains information about the file's name, size, modification date, matching results, and any errors encountered
    during analysis.

    :ivar filename: The name of the file.
    :ivar filesize: The size of the file in bytes.
    :ivar modified: The modification date of the file.
    :ivar errors: Any errors encountered during analysis.
    :ivar matches: The list of matches found for the file.
    """

    filename: Path
    filesize: int
    modified: datetime
    errors: str
    matches: list[SiegfriedMatch]

    def best_match(self) -> SiegfriedMatch | None:
        """
        Get the best match for the file.

        :return: A SiegfriedMatch object or None if there are no known matches.
        """
        matches: list[SiegfriedMatch] = [m for m in self.matches if m.id]
        matches.sort(key=lambda m: m.sort_tuple())
        return matches[-1] if matches else None

    def best_matches(self) -> list[SiegfriedMatch]:
        """
        Get the matches for the file sorted by how good they are; best are first.

        :return: A list of SiegfriedMatch objects.
        """
        return sorted([m for m in self.matches if m.id], key=lambda m: m.sort_tuple(), reverse=True)


class SiegfriedResult(BaseModel):
    """
    Represents the result of a Siegfried signature scan.

    :ivar siegfried: The version of Siegfried used for the scan.
    :ivar scandate: The date and time when the scan was performed.
    :ivar signature: The digital signature used for the scan.
    :ivar created: The date and time when the signature file was created.
    :ivar identifiers: A list of identifiers used for file identification.
    :ivar files: A list of files that were scanned.
    """

    siegfried: str
    scandate: datetime
    signature: str
    created: datetime
    identifiers: list[SiegfriedIdentifier]
    files: list[SiegfriedFile]
    model_config = ConfigDict(extra="forbid")

    @property
    def files_dict(self) -> dict[Path, SiegfriedFile]:
        return {f.filename: f for f in self.files}


class Siegfried:
    """
    A class for interacting with the Siegfried file identification tool.

    :ivar See Also: https://github.com/richardlehane/siegfried.
    """

    def __init__(
        self,
        binary: str | PathLike = "sf",
        signature: str = "default.sig",
        home: str | PathLike | None = None,
    ) -> None:
        """
        Initializes a new instance of the Siegfried class.

        :param binary: The path or name of the Siegfried binary, defaults to "sf".
        :param signature: The name of the signature file to use, defaults to "default.sig".
        :param home: The location of the Siegfried home folder, defaults to None.
        """
        self.binary: str = str(binary)
        self.signature: str = signature
        self.home: Path | None = Path(home) if home else None

    def run(self, *args: str) -> CompletedProcess:
        """
        Run the Siegfried command.

        :param args: The arguments to be given to Siegfried (excluding the binary path/name).
        :raises IdentificationError: If Siegfried exits with a non-zero status code.
        :return: A ``subprocess.CompletedProcess`` object.
        """
        if self.home:
            args = ("-home", str(self.home), *args)
        return _check_process(run([self.binary, *args], capture_output=True, encoding="utf-8"))  # noqa: PLW1510

    def update(self, signature: TSignaturesProvider | None = None, *, set_signature: bool = True):
        """
        Update or fetch signature files.

        :param signature: The name of signatures provider, one of: "pronom", "loc", "tika", "freedesktop",
            "pronom-tika-loc", "deluxe", "archivematica", defaults to the currently set signature.
        :param set_signature: Set to True to automatically change the signature to the newly updated one, defaults
            to True.
        :raises IdentificationError: If Siegfried exits with a non-zero status code.
        """
        if signature is not None and signature not in get_type_args(TSignaturesProvider):
            raise IdentificationError(f"Unknown signature provider {signature!r}")
        signature = signature.lower() if signature else self.signature.removesuffix(".sig")
        signature_file = f"{signature}.sig" if signature else self.signature

        self.run("-sig", signature_file, "-update", signature)

        if set_signature:
            self.signature = signature_file

    def identify(self, *path: str | PathLike) -> SiegfriedResult:
        """
        Identify a file.

        :param path: The path to the file.
        :raises IdentificationError: If there is an error calling Siegfried or processing its results.
        :return: A SiegfriedResult object
        """
        process: CompletedProcess = self.run("-sig", self.signature, "-json", "-multi", "1024", *map(str, path))
        try:
            return SiegfriedResult.model_validate_json(process.stdout)
        except ValueError as err:
            raise IdentificationError(err)
