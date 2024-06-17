from pathlib import Path
from typing import Any

from pydantic import BaseModel


class ACABase(BaseModel):
    def dump(self, to_file: Path) -> None:
        to_file.write_text(super().model_dump_json(), encoding="utf-8")

    def encode(self) -> Any:  # noqa: ANN401
        """Encode function."""
        return super().model_dump(mode="json")
