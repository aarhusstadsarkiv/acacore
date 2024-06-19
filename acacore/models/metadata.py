from pydantic import BaseModel

from acacore.__version__ import __version__


class Metadata(BaseModel):
    version: str = __version__
