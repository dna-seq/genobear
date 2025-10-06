"""
GenoBear pipelines package.

Contains subpackages for different types of genomic data pipelines:
- preparation: Download, convert, split, and upload genomic data
- annotation: (Future) Annotate genomic variants
"""

from genobear.pipelines.preparation import PreparationPipelines

__all__ = ["PreparationPipelines"]

