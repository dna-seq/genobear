# Configure multiprocessing early to avoid Polars fork() warnings
from . import multiprocessing_config

from genobear.io import get_info_fields, read_vcf_file, vcf_to_parquet, clean_extra_semicolons, AnnotatedResult, AnnotatedLazyFrame
from genobear.annotators import Annotator, VCFAnnotator
from genobear.downloaders import DbSNPDownloader, ClinVarDownloader, MultiVCFDownloader, EnsemblDownloader
from genobear.cli import app

__all__ = [
    "get_info_fields", 
    "read_vcf_file", 
    "vcf_to_parquet", 
    "clean_extra_semicolons",
    "AnnotatedResult", 
    "AnnotatedLazyFrame", 
    "Annotator", 
    "VCFAnnotator",
    "DbSNPDownloader",
    "ClinVarDownloader",
    "MultiVCFDownloader",
    "EnsemblDownloader",
    "app"
]