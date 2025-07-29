"""
GenoBear API - Programmatic interface for downloading and annotating genomic databases.

This module provides function-based APIs that can be used programmatically, 
alongside the CLI interfaces.
"""

import asyncio
from pathlib import Path
from typing import List, Dict, Optional

# Import all the database functions
from genobear.databases import (
    download_dbsnp, convert_dbsnp_to_parquet,
    download_clinvar, convert_clinvar_to_parquet,
    download_annovar,
    convert_hgmd_to_formats,
    download_refseq,
    download_exomiser
)

# Import annotation functions
from genobear.annotation import (
    annotate_vcf, annotate_vcf_batch,
    discover_databases, get_available_assemblies, get_available_releases
)

# Import configuration
from genobear.config import (
    DEFAULT_ASSEMBLY, DEFAULT_RELEASE, 
    get_database_folder, get_parquet_path, get_vcf_path,
    SUPPORTED_DATABASES
)

__all__ = [
    # Database download functions
    "download_dbsnp_sync", "download_clinvar_sync", "download_annovar_sync",
    "download_refseq_sync", "download_exomiser_sync",
    
    # Conversion functions  
    "convert_dbsnp_to_parquet_sync", "convert_clinvar_to_parquet_sync",
    "convert_hgmd_to_formats",
    
    # Annotation functions
    "annotate_vcf", "annotate_vcf_batch",
    
    # Discovery functions
    "discover_databases", "get_available_assemblies", "get_available_releases",
    
    # Configuration functions
    "get_database_folder", "get_parquet_path", "get_vcf_path",
    "list_supported_databases",
    
    # Constants
    "DEFAULT_ASSEMBLY", "DEFAULT_RELEASE", "SUPPORTED_DATABASES"
]


# Synchronous wrappers for async functions
def download_dbsnp_sync(
    assemblies: List[str],
    releases: List[str],
    output_folder: Optional[Path] = None,
    convert_to_parquet_files: bool = True,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download dbSNP databases synchronously.
    
    Args:
        assemblies: List of genome assemblies (e.g., ['hg38'])
        releases: List of dbSNP releases (e.g., ['b156'])
        output_folder: Output folder (uses config default if None)
        convert_to_parquet_files: Whether to convert to Parquet format
        force: Whether to force redownload
        
    Returns:
        Dictionary mapping file paths to results
    """
    return asyncio.run(download_dbsnp(
        assemblies=assemblies,
        releases=releases,
        output_folder=output_folder,
        convert_to_parquet_files=convert_to_parquet_files,
        force=force
    ))


def download_clinvar_sync(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    dated: bool = False,
    date_string: Optional[str] = None,
    convert_to_parquet_files: bool = True,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download ClinVar databases synchronously.
    
    Args:
        assemblies: List of genome assemblies (e.g., ['hg38'])
        output_folder: Output folder (uses config default if None)
        dated: Whether to download dated version
        date_string: Specific date for dated version
        convert_to_parquet_files: Whether to convert to Parquet format
        force: Whether to force redownload
        
    Returns:
        Dictionary mapping file paths to results
    """
    return asyncio.run(download_clinvar(
        assemblies=assemblies,
        output_folder=output_folder,
        dated=dated,
        date_string=date_string,
        convert_to_parquet_files=convert_to_parquet_files,
        force=force
    ))


def download_annovar_sync(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download ANNOVAR databases synchronously.
    
    Args:
        assemblies: List of genome assemblies
        output_folder: Output folder (uses config default if None)
        files: Specific files to download
        force: Whether to force redownload
        
    Returns:
        Dictionary mapping file paths to results
    """
    return asyncio.run(download_annovar(
        assemblies=assemblies,
        output_folder=output_folder,
        files=files,
        force=force
    ))


def download_refseq_sync(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
    format_to_bed: bool = True
) -> Dict[str, Dict[str, Optional[Path]]]:
    """
    Download RefSeq databases synchronously.
    
    Args:
        assemblies: List of genome assemblies
        output_folder: Output folder (uses config default if None)
        files: Specific files to download
        format_to_bed: Whether to format to BED format
        
    Returns:
        Dictionary mapping assemblies to file results
    """
    return asyncio.run(download_refseq(
        assemblies=assemblies,
        output_folder=output_folder,
        files=files,
        format_to_bed=format_to_bed
    ))


def download_exomiser_sync(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    exomiser_release: Optional[str] = None,
    phenotype_release: Optional[str] = None
) -> Dict[str, Dict[str, Optional[Path]]]:
    """
    Download Exomiser databases synchronously.
    
    Args:
        assemblies: List of genome assemblies
        output_folder: Output folder (uses config default if None)
        exomiser_release: Specific Exomiser release
        phenotype_release: Specific phenotype release
        
    Returns:
        Dictionary mapping assemblies to file results
    """
    return asyncio.run(download_exomiser(
        assemblies=assemblies,
        output_folder=output_folder,
        exomiser_release=exomiser_release,
        phenotype_release=phenotype_release
    ))


def convert_dbsnp_to_parquet_sync(
    assemblies: List[str],
    releases: List[str],
    output_folder: Optional[Path] = None,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Convert dbSNP VCF files to Parquet format synchronously.
    
    Args:
        assemblies: List of genome assemblies
        releases: List of dbSNP releases
        output_folder: Output folder (uses config default if None)
        force: Whether to force reconversion
        
    Returns:
        Dictionary mapping parquet paths to results
    """
    return asyncio.run(convert_dbsnp_to_parquet(
        assemblies=assemblies,
        releases=releases,
        output_folder=output_folder,
        force=force
    ))


def convert_clinvar_to_parquet_sync(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    dated: bool = False,
    date_string: Optional[str] = None,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Convert ClinVar VCF files to Parquet format synchronously.
    
    Args:
        assemblies: List of genome assemblies
        output_folder: Output folder (uses config default if None)
        dated: Whether files are dated versions
        date_string: Date string for dated versions
        force: Whether to force reconversion
        
    Returns:
        Dictionary mapping parquet paths to results
    """
    return asyncio.run(convert_clinvar_to_parquet(
        assemblies=assemblies,
        output_folder=output_folder,
        dated=dated,
        date_string=date_string,
        force=force
    ))


def list_supported_databases() -> Dict[str, str]:
    """
    Get list of supported databases with descriptions.
    
    Returns:
        Dictionary mapping database types to descriptions
    """
    return SUPPORTED_DATABASES.copy()


# Convenience functions for common use cases
def download_and_convert_dbsnp(
    assemblies: List[str] = [DEFAULT_ASSEMBLY],
    releases: List[str] = [DEFAULT_RELEASE],
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download and convert dbSNP databases in one step.
    
    Args:
        assemblies: List of genome assemblies
        releases: List of dbSNP releases
        force: Whether to force redownload/reconversion
        
    Returns:
        Dictionary with both VCF and Parquet file results
    """
    return download_dbsnp_sync(
        assemblies=assemblies,
        releases=releases,
        convert_to_parquet_files=True,
        force=force
    )


def download_and_convert_clinvar(
    assemblies: List[str] = [DEFAULT_ASSEMBLY],
    dated: bool = False,
    date_string: Optional[str] = None,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download and convert ClinVar databases in one step.
    
    Args:
        assemblies: List of genome assemblies
        dated: Whether to download dated version
        date_string: Specific date for dated version
        force: Whether to force redownload/reconversion
        
    Returns:
        Dictionary with both VCF and Parquet file results
    """
    return download_clinvar_sync(
        assemblies=assemblies,
        dated=dated,
        date_string=date_string,
        convert_to_parquet_files=True,
        force=force
    )