from datetime import datetime
from typing import Any

from pydantic import UUID4

from .base import ACABase


class HistoryEntry(ACABase):
    uuid: UUID4
    time: datetime
    operation: str
    data: Any | None = None
    reason: str | None = None
