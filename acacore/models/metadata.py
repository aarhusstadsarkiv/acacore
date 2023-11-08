from acacore.__version__ import __version__

from .base import ACABase


class Metadata(ACABase):
    version: str = __version__
