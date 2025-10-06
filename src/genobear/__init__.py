"""Top-level API for GenoBear.

Expose `PreparationPipelines` as the primary entrypoint for building/running
data preparation pipelines. `Pipelines` is kept as a backwards-compatible alias.
"""

from typing import Any
import warnings

from genobear.pipelines.preparation import PreparationPipelines

# Backwards compatibility: keep Pipelines as an alias
Pipelines = PreparationPipelines

__all__ = ["PreparationPipelines", "Pipelines"]


def __getattr__(name: str) -> Any:  # pragma: no cover
    """Provide a lightweight, deprecated alias without polluting exports.

    Accessing `genobear.PipelineFactory` will return `PreparationPipelines` with a
    DeprecationWarning. Prefer importing `PreparationPipelines` directly.
    """
    if name == "PipelineFactory":
        warnings.warn(
            "PipelineFactory is deprecated; use PreparationPipelines instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return PreparationPipelines
    raise AttributeError(f"module 'genobear' has no attribute {name!r}")
