from datetime import datetime
from typing import Optional

from pydantic import UUID4

from .base import ACABase


class HistoryEntry(ACABase):
    uuid: Optional[UUID4] = None
    time: datetime
    operation: str
    data: Optional[object] = None
    reason: Optional[str] = None
