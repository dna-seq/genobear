"""Top-level API for GenoBear.

Expose `Pipelines` as the primary entrypoint for building/running pipelines.
"""

from typing import Any
import warnings

from genobear.pipelines.helpers import Pipelines as Pipelines

__all__ = ["Pipelines"]


def __getattr__(name: str) -> Any:  # pragma: no cover
    """Provide a lightweight, deprecated alias without polluting exports.

    Accessing `genobear.PipelineFactory` will return `Pipelines` with a
    DeprecationWarning. Prefer importing `Pipelines` directly.
    """
    if name == "PipelineFactory":
        warnings.warn(
            "PipelineFactory is deprecated; use Pipelines instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return Pipelines
    raise AttributeError(f"module 'genobear' has no attribute {name!r}")
