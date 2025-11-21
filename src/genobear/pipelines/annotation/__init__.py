"""Annotation pipelines for genomic VCF files."""

from genobear.pipelines.annotation.runners import AnnotationPipelines
from genobear.pipelines.annotation.hf_dataset_downloader import (
    download_ensembl_from_hf,
    ensure_ensembl_cache,
)
from genobear.pipelines.annotation.vcf_parser import (
    extract_chromosomes_from_vcf,
    load_vcf_as_lazy_frame,
)
from genobear.pipelines.annotation.annotator import (
    find_annotation_parquet_files,
    annotate_vcf_with_ensembl,
    save_annotated_vcf,
)

__all__ = [
    "AnnotationPipelines",
    "download_ensembl_from_hf",
    "ensure_ensembl_cache",
    "extract_chromosomes_from_vcf",
    "load_vcf_as_lazy_frame",
    "find_annotation_parquet_files",
    "annotate_vcf_with_ensembl",
    "save_annotated_vcf",
]

