from datetime import datetime
from typing import Optional
from typing import Union

from pydantic import UUID4

from .base import ACABase


class HistoryEntry(ACABase):
    uuid: UUID4
    time: datetime
    operation: str
    data: Optional[Union[dict, list, str, int, float, bool, datetime]] = None
    reason: Optional[str] = None
