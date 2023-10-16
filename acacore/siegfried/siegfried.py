from datetime import datetime
from os import PathLike
from subprocess import CompletedProcess
from subprocess import run
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator

from acacore.exceptions.files import IdentificationError


def _check_process(process: CompletedProcess):
    """
    Raises:
        IdentificationError: if the process ends with a return code other than 0
    """
    if process.returncode != 0:
        raise IdentificationError(
            process.stderr or process.stdout or f"Unknown siegfried error code {process.returncode}"
        )


class SiegfriedIdentifier(BaseModel):
    name: str
    details: str


class SiegfriedMatch(BaseModel):
    ns: str
    id: Optional[str]
    format: str
    version: str
    mime: str
    match_class: str = Field(alias="class")
    basis: str
    warning: str

    # noinspection PyNestedDecorators
    @field_validator("id")
    @classmethod
    def unknown_id(cls, _id: Optional[str]):
        _id = (_id or "").strip()
        return None if _id.lower() == "unknown" else _id or None


class SiegfriedFile(BaseModel):
    filename: str
    filesize: int
    modified: datetime
    errors: str
    matches: list[SiegfriedMatch]


class SiegfriedResult(BaseModel):
    siegfried: str
    scandate: datetime
    signature: str
    created: datetime
    identifiers: list[SiegfriedIdentifier]
    files: list[SiegfriedFile]
    model_config = ConfigDict(extra="forbid")


class Siegfried:
    """
    A wrapper class to use the Siegfried program with Python and return the results with Pydantic models.

    See Also:
        https://github.com/richardlehane/siegfried
    """

    def __init__(self, binary: Union[str, PathLike] = "sf"):
        """
        Args:
            binary: The path to the Siegfried binary, or the program name if it is included in the PATH variable

        Raises:
            IdentificationError: If Siegfried is not configured properly
        """
        self.binary: str = str(binary)
        _check_process(run([self.binary, "-v"], capture_output=True, encoding="utf-8"))

    def identify(self, path: Union[str, PathLike]) -> SiegfriedResult:
        """
        Identify a file.

        Args:
            path: The path to the file

        Returns:
            A SiegfriedResult object

        Raises:
            IdentificationError: If there is an error calling Siegfried or processing its results
        """
        process: CompletedProcess = run(
            [self.binary, "-json", "-multi", "1024", str(path)],
            capture_output=True,
            encoding="utf-8",
        )
        _check_process(process)
        try:
            return SiegfriedResult.model_validate_json(process.stdout)
        except ValueError as err:
            raise IdentificationError(err)
