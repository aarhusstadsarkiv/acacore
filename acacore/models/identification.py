from typing import Any, Optional

from pydantic import root_validator

from .base import ACABase


class Identification(ACABase):
    """File identification datamodel."""

    puid: Optional[str]
    signature: Optional[str]
    warning: Optional[str]

    # noinspection PyNestedDecorators
    @root_validator(pre=True)
    @classmethod
    def check_puid_sig(cls, data: dict[Any, Any]) -> dict[Any, Any]:
        """Validate that a PUID cannot have an empty signature or vice versa."""

        puid, signature = data.get("puid"), data.get("signature")

        if puid is not None and signature is None:
            raise ValueError(f"Signature missing for PUID {puid}.")
        elif puid is None and signature is not None:
            raise ValueError(f"PUID missing for signature {signature}.")

        return data
