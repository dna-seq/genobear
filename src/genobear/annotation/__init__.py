"""
Annotation functionality for GenoBear.
"""

from genobear.annotation.annotate import annotate_vcf, annotate_vcf_batch
from genobear.annotation.discovery import discover_databases, get_available_assemblies, get_available_releases

__all__ = [
    "annotate_vcf", "annotate_vcf_batch",
    "discover_databases", "get_available_assemblies", "get_available_releases"
]