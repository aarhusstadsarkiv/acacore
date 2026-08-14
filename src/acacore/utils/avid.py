import re
from pathlib import Path
from typing import ClassVar


# noinspection PyPep8Naming
class AVIDIndices:
    """
    Class to compute paths to AVID index files.

    :ivar root: The AVID root directory.
    """

    def __init__(self, avid_dir: Path) -> None:
        self.root = avid_dir

    @property
    def path(self):
        """Indices"""  # noqa: D400
        return self.root / "Indices"

    @property
    def archiveIndex(self) -> Path:
        """Indices/archiveIndex.xml"""  # noqa: D400
        return self.path / "archiveIndex.xml"

    @property
    def contextDocumentationIndex(self) -> Path:
        """Indices/contextDocumentationIndex.xml"""  # noqa: D400
        return self.path / "contextDocumentationIndex.xml"

    @property
    def docIndex(self) -> Path:
        """Indices/docIndex.xml"""  # noqa: D400
        return self.path / "docIndex.xml"

    @property
    def fileIndex(self) -> Path:
        """Indices/fileIndex.xml"""  # noqa: D400
        return self.path / "fileIndex.xml"

    @property
    def tableIndex(self) -> Path:
        """Indices/tableIndex.xml"""  # noqa: D400
        return self.path / "tableIndex.xml"


# noinspection PyPep8Naming
class AVIDSchemas:
    """
    Class to compute paths to AVID schema files.

    :ivar root: The AVID root directory.
    """

    def __init__(self, avid_dir: Path) -> None:
        self.root: Path = avid_dir

    @property
    def path(self):
        """Schemas"""  # noqa: D400
        return self.root / "Schemas"

    @property
    def archiveIndex(self) -> Path:
        """Schemas/standard/archiveIndex.xsd"""  # noqa: D400
        return self.path / "standard" / "archiveIndex.xsd"

    @property
    def contextDocumentationIndex(self) -> Path:
        """Schemas/standard/contextDocumentationIndex.xsd"""  # noqa: D400
        return self.path / "standard" / "contextDocumentationIndex.xsd"

    @property
    def docIndex(self) -> Path:
        """Schemas/standard/docIndex.xsd"""  # noqa: D400
        return self.path / "standard" / "docIndex.xsd"

    @property
    def fileIndex(self) -> Path:
        """Schemas/standard/fileIndex.xsd"""  # noqa: D400
        return self.path / "standard" / "fileIndex.xsd"

    @property
    def researchIndex(self) -> Path:
        """Schemas/standard/researchIndex.xsd"""  # noqa: D400
        return self.path / "standard" / "researchIndex.xsd"

    @property
    def tableIndex(self) -> Path:
        """Schemas/standard/tableIndex.xsd"""  # noqa: D400
        return self.path / "standard" / "tableIndex.xsd"

    @property
    def XMLSchema(self) -> Path:
        """Schemas/standard/XMLSchema.xsd"""  # noqa: D400
        return self.path / "standard" / "XMLSchema.xsd"

    @property
    def tables(self) -> dict[int, Path]:
        """Tables/tableN/tableN.xsd"""  # noqa: D400
        return {
            int(f.name.removeprefix("table")): f.joinpath(f.name).with_suffix(".xsd")
            for f in self.root.joinpath("Tables").iterdir()
            if f.is_dir() and re.match(r"^table\d+$", f.name)
        }


class AVIDDirs:
    """
    Class to compute AVID directory paths.

    :ivar root: The AVID root directory.
    :ivar standalone: ``True`` if the AVID is a standalone directory, ``False`` otherwise.
    """

    def __init__(self, avid_dir: Path, standalone: bool = False) -> None:
        self.root: Path = avid_dir
        self.standalone: bool = standalone

    @property
    def original_documents(self):
        """OriginalDocuments"""  # noqa: D400
        if self.standalone:
            return self.root
        return self.root / "OriginalDocuments"

    @property
    def master_documents(self):
        """MasterDocuments"""  # noqa: D400
        return self.root / "MasterDocuments"

    @property
    def access_documents(self):
        """AccessDocuments"""  # noqa: D400
        return self.root / "AccessDocuments"

    @property
    def documents(self):
        """Documents"""  # noqa: D400
        return self.root / "Documents"

    @property
    def context_documentation(self):
        """ContextDocumentation"""  # noqa: D400
        return self.root / "ContextDocumentation"

    @property
    def indices(self) -> AVIDIndices:
        """Indices"""  # noqa: D400
        return AVIDIndices(self.root)

    @property
    def schemas(self) -> AVIDSchemas:
        """Schemas"""  # noqa: D400
        return AVIDSchemas(self.root)

    @property
    def tables(self) -> Path:
        """Tables"""  # noqa: D400
        return self.root / "Tables"

    @property
    def tables_dict(self) -> dict[int, Path]:
        """Tables"""  # noqa: D400
        return {
            int(f.name.removeprefix("table")): f.joinpath(f.name).with_suffix(".xml")
            for f in self.root.joinpath("Tables").iterdir()
            if f.is_dir() and re.match(r"^table\d+$", f.name)
        }


class AVID:
    """
    Class to handle AVID paths.

    :ivar path: Path to the AVID root directory.
    :ivar dirs: AVID directories' handler.
    """

    database_name: ClassVar[str] = "avid.db"

    def __init__(self, directory: str | Path, standalone: bool = False) -> None:
        if not standalone and not self.is_avid_dir(directory):
            raise ValueError(f"{directory} is not a valid AVID directory")

        self.path: Path = Path(directory).resolve()
        self.dirs: AVIDDirs = AVIDDirs(self.path, standalone)

    def __str__(self) -> str:
        return str(self.path)

    @classmethod
    def create(cls, directory: str | Path) -> "AVID":
        """
        Create AVID root directories.

        :param directory: The directory to use as root.
        :return: An instance of ``AVID``.
        """
        dirs = AVIDDirs(Path(directory))

        dirs.original_documents.mkdir(parents=True, exist_ok=True)
        dirs.master_documents.mkdir(parents=True, exist_ok=True)
        dirs.access_documents.mkdir(parents=True, exist_ok=True)
        dirs.documents.mkdir(parents=True, exist_ok=True)
        dirs.context_documentation.mkdir(parents=True, exist_ok=True)
        dirs.indices.path.mkdir(parents=True, exist_ok=True)
        dirs.schemas.path.mkdir(parents=True, exist_ok=True)
        dirs.tables.mkdir(parents=True, exist_ok=True)

        return cls(directory)

    @classmethod
    def is_avid_dir(cls, directory: str | Path) -> bool:
        """
        Check if the given directory is a valid AVID directory.

        To be valid, it needs to contain an "Indices" folder, a "Schemas" folders, and either an "OriginalDocuments"
        or "Documents" folder.

        :param directory: The directory to check.
        :return: ``True`` if the directory is valid, ``False`` otherwise.
        """
        directory = Path(directory)
        if not directory.is_dir():
            return False
        if not (avid_dirs := AVIDDirs(directory)).indices.path.is_dir():
            return False
        if not avid_dirs.schemas.path.is_dir():
            return False
        if not avid_dirs.original_documents.is_dir() and not avid_dirs.documents.is_dir():  # noqa: SIM103
            return False
        return True

    @classmethod
    def find_database_root(cls, directory: str | Path) -> Path | None:
        """
        Find the AVID root in the given directory or any of its parents.

        The AVID root is identified by a "_metadata" directory containing an AVID database.

        :param directory: The directory from which to start the search.
        :return: The AVID root directory, if any is found, else ``None``.
        """
        directory = Path(directory)
        if directory.joinpath("_metadata", cls.database_name).is_file():
            return directory
        if directory.parent != directory:
            return cls.find_database_root(directory.parent)
        return None

    @property
    def metadata_dir(self):
        """_metadata"""  # noqa: D400
        return self.path / "_metadata"

    @property
    def database_path(self):
        """_metadata/<database>"""  # noqa: D400
        return self.metadata_dir / self.database_name
