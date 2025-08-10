"""
Genobear annotation system.

This package provides a generic, dependency-aware annotation framework
that allows chaining of different data transformation steps.
"""

from genobear.annotators.base_annotator import Annotator
from genobear.annotators.vcf_annotator import VCFAnnotator

__all__ = ["Annotator", "VCFAnnotator"]
