from pathlib import Path
from typing import Any, ClassVar, Optional

from pydantic import model_validator

from acacore.database.files_db import FileDB

from .base import ACABase
from .file import ArchiveFile


# noinspection PyNestedDecorators
class FileData(ACABase):
    main_dir: Path
    data_dir: Optional[Path] = None
    db: Optional[FileDB] = None
    files: ClassVar[list[ArchiveFile]] = []

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="before")
    @classmethod
    def create_dir(cls, data: dict[str, Any]) -> dict[str, Any]:
        main_dir = data.get("main_dir")
        data_dir = data.get("data_dir")
        db = data.get("db")
        if data_dir is None and main_dir:
            data_dir = main_dir / "_metadata"
            data_dir.mkdir(exist_ok=True)
            data["data_dir"] = data_dir
        if db is None and data_dir:
            data["db"] = FileDB(f"sqlite:///{data['data_dir'] / 'files.db'}")
        return data
