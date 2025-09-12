"""
Pipeline helper functions for common genomic data sources.

This module provides convenient functions to quickly construct pipelines
for popular genomic databases like ClinVar, Ensembl, dbSNP, etc.
"""

import concurrent
import os
from pathlib import Path
from typing import Optional

from pipefunc import Pipeline
from eliot import start_action

from genobear.pipelines.vcf_downloader import make_vcf_pipeline
from genobear.pipelines.vcf_parquet_splitter import make_vcf_splitting_pipeline, make_parquet_splitting_pipeline
from pycomfort.logging import to_nice_stdout, to_nice_file
from genobear.config import get_default_workers
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

def make_clinvar_pipeline(with_splitting: bool = False) -> Pipeline:
    """Create a pipeline for downloading and processing ClinVar data.
    
    Args:
        with_splitting: If True, includes variant splitting by TSA
        
    Returns:
        Configured pipeline with ClinVar defaults
    """
    if with_splitting:
        pipeline = make_vcf_splitting_pipeline()
    else:
        pipeline = make_vcf_pipeline()
    
    # Set ClinVar defaults
    clinvar_defaults = {
        "url": "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/",
        "pattern": r"clinvar\.vcf\.gz$",
        "name": "clinvar",
        "file_only": True,
        "check_files": True,
        "expiry_time": 7 * 24 * 3600,  # 7 days
    }
    
    if with_splitting:
        clinvar_defaults["explode_snv_alt"] = True
    
    pipeline.update_defaults(clinvar_defaults)
    return pipeline


def make_ensembl_pipeline(with_splitting: bool = False) -> Pipeline:
    """Create a pipeline for downloading and processing Ensembl VCF data.
    
    Args:
        with_splitting: If True, includes variant splitting by TSA
        
    Returns:
        Configured pipeline with Ensembl defaults
    """
    if with_splitting:
        pipeline = make_vcf_splitting_pipeline()
    else:
        pipeline = make_vcf_pipeline()
    
    # Set Ensembl VCF defaults
    ensembl_defaults = {
        "url": "https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/",
        "pattern": r"homo_sapiens-chr([^.]+)\.vcf\.gz$",
        "name": "ensembl_vcf",
        "file_only": True,
        "check_files": True,
        "expiry_time": 7 * 24 * 3600,  # 7 days
    }
    
    if with_splitting:
        ensembl_defaults["explode_snv_alt"] = True
    
    pipeline.update_defaults(ensembl_defaults)
    return pipeline


def make_dbsnp_pipeline(with_splitting: bool = False, build: str = "GRCh38") -> Pipeline:
    """Create a pipeline for downloading and processing dbSNP data.
    
    Args:
        with_splitting: If True, includes variant splitting by TSA
        build: Genome build (GRCh38 or GRCh37)
        
    Returns:
        Configured pipeline with dbSNP defaults
    """
    if with_splitting:
        pipeline = make_vcf_splitting_pipeline()
    else:
        pipeline = make_vcf_pipeline()
    
    # Set dbSNP VCF defaults based on build
    if build == "GRCh38":
        base_url = "https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/"
        pattern = r"GCF_000001405\.38\.gz$"
    elif build == "GRCh37":
        base_url = "https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/"
        pattern = r"GCF_000001405\.25\.gz$"
    else:
        raise ValueError(f"Unsupported build: {build}. Use 'GRCh38' or 'GRCh37'")
    
    dbsnp_defaults = {
        "url": base_url,
        "pattern": pattern,
        "name": f"dbsnp_{build.lower()}",
        "file_only": True,
        "check_files": True,
        "expiry_time": 30 * 24 * 3600,  # 30 days (dbSNP changes less frequently)
    }
    
    if with_splitting:
        dbsnp_defaults["explode_snv_alt"] = True
    
    pipeline.update_defaults(dbsnp_defaults)
    return pipeline


def make_gnomad_pipeline(with_splitting: bool = False, version: str = "v4") -> Pipeline:
    """Create a pipeline for downloading and processing gnomAD data.
    
    Args:
        with_splitting: If True, includes variant splitting by TSA
        version: gnomAD version (v3, v4)
        
    Returns:
        Configured pipeline with gnomAD defaults
    """
    if with_splitting:
        pipeline = make_vcf_splitting_pipeline()
    else:
        pipeline = make_vcf_pipeline()
    
    # Set gnomAD VCF defaults based on version
    if version == "v4":
        base_url = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/4.0/vcf/"
        pattern = r"gnomad\.v4\.0\..+\.vcf\.bgz$"
    elif version == "v3":
        base_url = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/3.1.2/vcf/"
        pattern = r"gnomad\.v3\.1\.2\..+\.vcf\.bgz$"
    else:
        raise ValueError(f"Unsupported version: {version}. Use 'v3' or 'v4'")
    
    gnomad_defaults = {
        "url": base_url,
        "pattern": pattern,
        "name": f"gnomad_{version}",
        "file_only": True,
        "check_files": True,
        "expiry_time": 30 * 24 * 3600,  # 30 days
    }
    
    if with_splitting:
        gnomad_defaults["explode_snv_alt"] = True
    
    pipeline.update_defaults(gnomad_defaults)
    return pipeline


# Convenience functions for quick data processing
def download_clinvar(
    dest_dir: Optional[Path] = None,
    with_splitting: bool = False,
    workers: Optional[int] = None,
    log: bool = True,
    timeout: Optional[float] = None,
    **kwargs
) -> dict:
    """Quick function to download and process ClinVar data.
    
    Args:
        dest_dir: Destination directory for downloads
        with_splitting: Whether to split variants by TSA
        workers: Number of parallel workers (default from env or cpu_count)
        timeout: Timeout in seconds for downloads (default 1 hour)
        log: Enable logging
        **kwargs: Additional arguments passed to pipeline.map()
        
    Returns:
        Pipeline results dictionary
    """
    if workers is None: 
        workers = int(os.getenv("GENOBEAR_DOWNLOAD_WORKERS", os.cpu_count()))
    timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
    
    if log:
        to_nice_stdout()
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        to_nice_file(log_dir / "download_clinvar.json", log_dir / "download_clinvar.log")
    pipeline = make_clinvar_pipeline(with_splitting=with_splitting)
    
    inputs = {
        "timeout": timeout,
        **kwargs
    }
    if dest_dir is not None:
        inputs["dest_dir"] = dest_dir
    
    if with_splitting:
        output_names = {"vcf_parquet_path", "split_variants_dict"}
    else:
        output_names = {"vcf_lazy_frame", "vcf_parquet_path"}
    
    with start_action(action_type="download_clinvar", with_splitting=with_splitting, dest_dir=str(dest_dir) if dest_dir else None):
        results = pipeline.map(
            inputs=inputs,
            output_names=output_names,
            parallel=workers > 1,
            return_results=True,
            show_progress="rich",
            executor=concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        )
        return results


def download_ensembl(
    dest_dir: Optional[Path] = None,
    with_splitting: bool = False,
    workers: Optional[int] = None,
    log: bool = True,
    timeout: Optional[float] = None,
    **kwargs
) -> dict:
    """Quick function to download and process Ensembl VCF data.
    
    Args:
        dest_dir: Destination directory for downloads
        with_splitting: Whether to split variants by TSA
        parallel: Run in parallel
        timeout: Timeout in seconds for downloads (default 30 minutes)
        **kwargs: Additional arguments passed to pipeline.map()
        
    Returns:
        Pipeline results dictionary
    """
    if workers is None: 
        workers = int(os.getenv("GENOBEAR_DOWNLOAD_WORKERS", os.cpu_count()))
    timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
    pipeline = make_ensembl_pipeline(with_splitting=with_splitting)
    
    # Provide the required inputs explicitly
    inputs = {
        "url": "https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/",
        "pattern": r"homo_sapiens-chr([^.]+)\.vcf\.gz$",
        "name": "ensembl_vcf",
        "file_only": True,
        "check_files": True,
        "expiry_time": 7 * 24 * 3600,  # 7 days
        "timeout": timeout,
        **kwargs
    }
    
    if dest_dir is not None:
        inputs["dest_dir"] = dest_dir
    
    if with_splitting:
        inputs["explode_snv_alt"] = True
        output_names = {"vcf_parquet_path", "split_variants_dict"}
    else:
        output_names = {"vcf_lazy_frame", "vcf_parquet_path"}

    if log:
        to_nice_stdout()
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        to_nice_file(log_dir / "download_ensembl.json", log_dir / "download_ensembl.log")
    
    with start_action(action_type="download_ensembl", with_splitting=with_splitting, dest_dir=str(dest_dir) if dest_dir else None):
        results = pipeline.map(
            inputs=inputs,
            output_names=output_names,
            parallel=workers > 1,
            return_results=True,
            show_progress="rich",
            executor=concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        )
        return results


def download_dbsnp(
    dest_dir: Optional[Path] = None,
    build: str = "GRCh38",
    with_splitting: bool = False,
    workers: Optional[int] = None,
    log: bool = True,
    timeout: Optional[float] = None,
    **kwargs
) -> dict:
    """Quick function to download and process dbSNP data.
    
    Args:
        dest_dir: Destination directory for downloads
        build: Genome build (GRCh38 or GRCh37)
        with_splitting: Whether to split variants by TSA
        workers: Number of parallel workers (default from env or cpu_count)
        timeout: Timeout in seconds for downloads (default 1 hour)
        log: Enable logging
        **kwargs: Additional arguments passed to pipeline.map()
        
    Returns:
        Pipeline results dictionary
    """
    if workers is None: 
        workers = int(os.getenv("GENOBEAR_DOWNLOAD_WORKERS", os.cpu_count()))
    timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
    
    pipeline = make_dbsnp_pipeline(with_splitting=with_splitting, build=build)
    
    inputs = {
        "timeout": timeout,
        **kwargs
    }
    if dest_dir is not None:
        inputs["dest_dir"] = dest_dir
    
    if with_splitting:
        output_names = {"vcf_parquet_path", "split_variants_dict"}
    else:
        output_names = {"vcf_lazy_frame", "vcf_parquet_path"}
    
    if log:
        to_nice_stdout()
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        to_nice_file(log_dir / "download_dbsnp.json", log_dir / "download_dbsnp.log")
    
    with start_action(action_type="download_dbsnp", with_splitting=with_splitting, build=build, dest_dir=str(dest_dir) if dest_dir else None):
        results = pipeline.map(
            inputs=inputs,
            output_names=output_names,
            parallel=workers > 1,
            return_results=True,
            show_progress="rich",
            executor=concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        )
        return results


def download_gnomad(
    dest_dir: Optional[Path] = None,
    version: str = "v4",
    with_splitting: bool = False,
    workers: Optional[int] = None,
    log: bool = True,
    timeout: Optional[float] = None,
    **kwargs
) -> dict:
    """Quick function to download and process gnomAD data.
    
    Args:
        dest_dir: Destination directory for downloads
        version: gnomAD version (v3, v4)
        with_splitting: Whether to split variants by TSA
        workers: Number of parallel workers (default from env or cpu_count)
        timeout: Timeout in seconds for downloads (default 1 hour)
        log: Enable logging
        **kwargs: Additional arguments passed to pipeline.map()
        
    Returns:
        Pipeline results dictionary
    """
    if workers is None: 
        workers = int(os.getenv("GENOBEAR_DOWNLOAD_WORKERS", os.cpu_count()))
    timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
    
    pipeline = make_gnomad_pipeline(with_splitting=with_splitting, version=version)
    
    inputs = {
        "timeout": timeout,
        **kwargs
    }
    if dest_dir is not None:
        inputs["dest_dir"] = dest_dir
    
    if with_splitting:
        output_names = {"vcf_parquet_path", "split_variants_dict"}
    else:
        output_names = {"vcf_lazy_frame", "vcf_parquet_path"}
    
    if log:
        to_nice_stdout()
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        to_nice_file(log_dir / "download_gnomad.json", log_dir / "download_gnomad.log")
    
    with start_action(action_type="download_gnomad", with_splitting=with_splitting, version=version, dest_dir=str(dest_dir) if dest_dir else None):
        results = pipeline.map(
            inputs=inputs,
            output_names=output_names,
            parallel=workers > 1,
            return_results=True,
            show_progress="rich",
            executor=concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        )
        return results


def split_existing_parquets(
    parquet_files: list[Path] | Path,
    explode_snv_alt: bool = True,
    write_to: Optional[Path] = None,
    workers: Optional[int] = None,
    log: bool = True,
    **kwargs
) -> dict:
    """Quick function to split existing parquet files by variant type.
    
    Args:
        parquet_files: Path or list of paths to parquet files
        explode_snv_alt: Whether to explode ALT column for SNV variants
        write_to: Output directory for split files
        workers: Number of parallel workers (default from env or cpu_count)
        log: Enable logging
        **kwargs: Additional arguments passed to pipeline.map()
        
    Returns:
        Pipeline results dictionary
    """
    if workers is None:
        workers = get_default_workers()
    
    pipeline = make_parquet_splitting_pipeline()
    
    if isinstance(parquet_files, Path):
        parquet_files = [parquet_files]
    
    inputs = {
        "vcf_parquet_path": parquet_files,
        "explode_snv_alt": explode_snv_alt,
        "write_to": write_to,
        **kwargs
    }
    
    if log:
        to_nice_stdout()
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        to_nice_file(log_dir / "split_parquets.json", log_dir / "split_parquets.log")
    
    with start_action(action_type="split_existing_parquets", num_files=len(parquet_files), explode_snv_alt=explode_snv_alt):
        results = pipeline.map(
            inputs=inputs,
            output_names={"split_variants_dict"},
            parallel=workers > 1,
            return_results=True,
            show_progress="rich",
            executor=concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        )
        return results