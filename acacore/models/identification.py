from typing import Any

from pydantic import model_validator

from .base import ACABase


class Identification(ACABase):
    """File identification datamodel."""

    puid: str | None
    signature: str | None
    warning: str | None
    size: int | None

    # noinspection PyNestedDecorators
    @model_validator(mode="before")
    @classmethod
    def check_puid_sig(cls, data: dict[Any, Any]) -> dict[Any, Any]:
        """Validate that a PUID cannot have an empty signature or vice versa."""
        puid, signature = data.get("puid"), data.get("signature")

        if puid is not None and signature is None:
            raise ValueError(f"Signature missing for PUID {puid}.")
        elif puid is None and signature is not None:  # noqa: RET506
            raise ValueError(f"PUID missing for signature {signature}.")

        return data
