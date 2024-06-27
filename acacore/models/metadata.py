from pydantic import BaseModel

from acacore.__version__ import __version__


class Metadata(BaseModel):
    """Metadata model containing information about the database and acacore version."""

    version: str = __version__
