from typing import Any, Callable, Self

from pydantic import BaseModel, model_serializer


class NoDefaultsModel(BaseModel):
    """A subclass of BaseModel that implements a custom serializer which excludes default values."""

    @model_serializer(mode="wrap", when_used="always")
    def _model_serializer(
        self, wrap: Callable[[Self], dict[str, Any]]
    ) -> dict[str, Any]:
        return {
            k: v
            for k, v in wrap(self).items()
            if k in self.model_fields_set and v != self.model_fields[k].default
        }
