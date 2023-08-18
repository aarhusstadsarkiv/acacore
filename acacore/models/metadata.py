from datetime import datetime
from pathlib import Path
from typing import Optional

from .base import ACABase


class Metadata(ACABase):
    """Data class for keeping track of metadata used in data.json."""

    last_run: datetime
    processed_dir: Path
    file_count: Optional[int] = None
    total_size: Optional[str] = None
