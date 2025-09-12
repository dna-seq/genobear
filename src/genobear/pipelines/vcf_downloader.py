import os
import re
import shutil
from pathlib import Path
from typing import List, Optional

import aiohttp
import fsspec
import polars as pl
from aiohttp import ClientResponseError, ClientTimeout
from eliot import start_action
from fsspec.exceptions import FSTimeoutError
from pipefunc import Pipeline, pipefunc
from platformdirs import user_cache_dir
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from genobear.io import AnnotatedLazyFrame, vcf_to_parquet

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


def _retryable_http_error(exc: BaseException) -> bool:
    # Network/client-level errors and timeouts are retryable
    if isinstance(exc, (aiohttp.ClientError, FSTimeoutError, TimeoutError, OSError)):
        return True
    # Response-level status codes that are commonly transient
    if isinstance(exc, ClientResponseError):
        return exc.status in RETRYABLE_STATUS
    return False


@pipefunc(output_name="urls")
def list_paths(url: str, pattern: str | None = None, file_only: bool = True) -> list[str]:
    fs, path = fsspec.core.url_to_fs(url)  # infer filesystem and strip protocol
    paths = fs.glob(path) if any(ch in path for ch in "*?[]") else [
        e["name"] for e in fs.ls(path, detail=True) if (not file_only) or e.get("type") == "file"
    ]
    if pattern:
        rx = re.compile(pattern)
        paths = [p for p in paths if rx.search(p.rsplit("/", 1)[-1])]
    return paths

@pipefunc(
    output_name="vcf_local",
    renames={"url": "urls"},
    mapspec="urls[i] -> vcf_local[i]",
)
def download_path(
    url: str,
    name: str | Path = "downloads",
    dest_dir: Path | None = None,
    cache_storage: Path | None = None,
    check_files: bool = True,
    expiry_time: int | float | None = 7 * 24 * 3600,  # 7 days
    timeout: float | None = None,  # seconds
    connect_timeout: float | None = 10.0,
    sock_read_timeout: float | None = 120.0,
    retries: int = 6,
    use_blockcache: bool = True,
    chunk_size: int = 8 * 1024 * 1024,  # 8MB
) -> Path:
    """
    Robust HTTP/HTTPS downloader with fsspec blockcache + Tenacity retry/backoff
    and atomic finalization to dest; resumable via blockcache's persisted ranges.
    """
    timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0) if timeout is None else timeout)

    user_cache_path = Path(user_cache_dir(appname="genobear"))
    # Track whether the caller explicitly provided dest_dir to reflect it in logs
    dest_dir_was_provided = dest_dir is not None
    if dest_dir is None:
        dest_dir = user_cache_path / name if isinstance(name, str) else Path(name)

    if cache_storage is None:
        if Path(dest_dir).resolve().is_relative_to(user_cache_path.resolve()):
            cache_storage = dest_dir
        else:
            cache_storage = user_cache_path / ".fsspec_cache"

    dest_dir = Path(dest_dir)
    cache_storage = Path(cache_storage)
    dest_dir.mkdir(parents=True, exist_ok=True)
    cache_storage.mkdir(parents=True, exist_ok=True)

    local = dest_dir / url.rsplit("/", 1)[-1]
    tmp = local.with_suffix(local.suffix + ".part")

    cache_proto = "blockcache" if use_blockcache else "filecache"
    chained_url = f"{cache_proto}::{url}"

    http_key = "https" if url.startswith("https://") else "http"
    client_timeout = ClientTimeout(total=timeout, connect=connect_timeout, sock_read=sock_read_timeout)
    http_layer = {"client_kwargs": {"timeout": client_timeout}}

    storage_options = {
        cache_proto: {
            "cache_storage": str(cache_storage),
            "check_files": check_files,   # filecache-only; harmless for blockcache
            "expiry_time": expiry_time,   # filecache-only; harmless for blockcache
        },
        http_key: http_layer,
    }

    def _download_to_tmp_with_retry(destination_tmp: Path) -> None:
        retryer = Retrying(
            retry=retry_if_exception(_retryable_http_error),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=30.0),
            stop=stop_after_attempt(retries),
            reraise=True,
        )

        if destination_tmp.exists():
            destination_tmp.unlink()

        for attempt in retryer:
            with attempt:
                with fsspec.open(chained_url, mode="rb", **storage_options) as src, open(destination_tmp, "wb") as dst:
                    while True:
                        data = src.read(chunk_size)
                        if not data:
                            break
                        dst.write(data)

    action_kwargs = {"action_type": "download_path", "url": url, "dest": str(local)}
    if dest_dir_was_provided:
        action_kwargs["dest_dir"] = str(dest_dir)

    with start_action(**action_kwargs) as action:
        action.log(message_type="info", step="start_download", url=url, timeout=timeout)

        # Stream from cached reader to tmp, then atomically move into dest_dir
        _download_to_tmp_with_retry(tmp)
        tmp.replace(local)

        action.log(message_type="info", step="download_finished", final_path=str(local))
        return local
        
@pipefunc(
    output_name=("vcf_lazy_frame", "vcf_parquet_path"),
    renames={"vcf_path": "vcf_local"},
    mapspec="vcf_local[i] -> vcf_lazy_frame[i], vcf_parquet_path[i]",
)
def convert_to_parquet(vcf_path: Path, overwrite: bool = False) -> AnnotatedLazyFrame:
    """Convert a VCF file to Parquet using io utilities.

    If the provided path does not look like a VCF (e.g., it's an index file like
    .tbi), the function skips conversion and returns the original path.
    """
    with start_action(action_type="convert_to_parquet", vcf_path=str(vcf_path)) as action:
        import polars as pl
        
        suffixes = vcf_path.suffixes
        is_index = suffixes and suffixes[-1] in [".tbi", ".csi", ".idx"]
        is_vcf = (".vcf" in suffixes) and not is_index

        if not is_vcf:
            action.log(message_type="info", step="skip_non_vcf", path=str(vcf_path))
            # Return empty LazyFrame and the original path for non-VCF files
            empty_lazy = pl.LazyFrame()
            return empty_lazy, vcf_path

        lazy_frame, parquet_path = vcf_to_parquet(vcf_path=vcf_path, overwrite=overwrite)
        action.log(
            message_type="info",
            step="conversion_complete",
            parquet_path=str(parquet_path),
        )
        return lazy_frame, parquet_path


def make_vcf_pipeline() -> Pipeline:
    return Pipeline([list_paths, download_path, convert_to_parquet], print_error=True)

def make_vcf_splitting_pipeline() -> Pipeline:
    """Create a pipeline that downloads, converts VCF files to parquet, and splits variants by TSA."""
    from genobear.pipelines.vcf_parquet_splitter import make_parquet_splitting_pipeline
    
    # Compose the VCF download pipeline with the splitting pipeline
    vcf_pipeline = make_vcf_pipeline()
    splitting_pipeline = make_parquet_splitting_pipeline()
    
    # Use the | operator to compose pipelines
    return vcf_pipeline | splitting_pipeline

def make_clinvar_pipeline() -> Pipeline:
    """Create a pipeline with ClinVar-specific defaults."""
    from genobear.pipelines.helpers import make_clinvar_pipeline as make_clinvar_helper
    return make_clinvar_helper(with_splitting=False)

def make_ensembl_vcf_pipeline() -> Pipeline:
    """Create a pipeline with Ensembl VCF-specific defaults."""
    from genobear.pipelines.helpers import make_ensembl_pipeline as make_ensembl_helper
    return make_ensembl_helper(with_splitting=False)

def make_clinvar_splitting_pipeline() -> Pipeline:
    """Create a pipeline with ClinVar-specific defaults and variant splitting."""
    from genobear.pipelines.helpers import make_clinvar_pipeline as make_clinvar_helper
    return make_clinvar_helper(with_splitting=True)

def make_ensembl_vcf_splitting_pipeline() -> Pipeline:
    """Create a pipeline with Ensembl VCF-specific defaults and variant splitting."""
    from genobear.pipelines.helpers import make_ensembl_pipeline as make_ensembl_helper
    return make_ensembl_helper(with_splitting=True)

def download_and_convert_vcf(
    url: str,
    pattern: str | None = None,
    name: str | Path = "downloads",
    output_names: set[str] | None = None,
    parallel: bool = True,
    return_results: bool = True,
    **kwargs
) -> dict:
    """Download and convert VCF files using the VCF pipeline.
    
    Args:
        url: Base URL to search for VCF files
        pattern: Regex pattern to filter files (optional)
        name: Directory name or Path for downloads
        output_names: Which outputs to return (defaults to lazy_frame and parquet_path only)
        parallel: Run pipeline in parallel (default True)
        return_results: Return results dict (default True)
        **kwargs: Additional parameters passed to pipeline.map()
    
    Returns:
        Dictionary with pipeline results containing 'vcf_lazy_frame' and 'vcf_parquet_path' by default
    """
    pipeline = make_vcf_pipeline()
    
    # Default to only the most useful outputs
    if output_names is None:
        output_names = {"vcf_lazy_frame", "vcf_parquet_path"}
    
    inputs = {
        "url": url,
        "pattern": pattern,
        "file_only": True,
        "name": name,
        "check_files": True,
        "expiry_time": 7 * 24 * 3600,  # 7 days
        **kwargs
    }
    
    with start_action(action_type="download_and_convert_vcf", url=url, name=str(name)):
        results = pipeline.map(
            inputs=inputs,
            output_names=output_names,
            parallel=parallel,
            return_results=return_results,
        )
        return results

def download_convert_and_split_vcf(
    url: str,
    pattern: str | None = None,
    name: str | Path = "downloads",
    output_names: set[str] | None = None,
    parallel: bool = True,
    return_results: bool = True,
    explode_snv_alt: bool = True,
    **kwargs
) -> dict:
    """Download, convert VCF files to parquet, and split variants by TSA using the VCF splitting pipeline.
    
    Args:
        url: Base URL to search for VCF files
        pattern: Regex pattern to filter files (optional)
        name: Directory name or Path for downloads
        output_names: Which outputs to return (defaults to parquet_path and split_variants_dict)
        parallel: Run pipeline in parallel (default True)
        return_results: Return results dict (default True)
        explode_snv_alt: Whether to explode ALT column on "|" separator for SNV variants
        **kwargs: Additional parameters passed to pipeline.map()
    
    Returns:
        Dictionary with pipeline results containing 'vcf_parquet_path' and 'split_variants_dict' by default
    """
    from genobear.pipelines.vcf_parquet_splitter import download_convert_and_split_vcf as download_split
    return download_split(
        url=url,
        pattern=pattern,
        name=name,
        output_names=output_names,
        parallel=parallel,
        return_results=return_results,
        explode_snv_alt=explode_snv_alt,
        **kwargs
    )

if __name__ == "__main__":
    # Example 1: Simple ClinVar download and conversion
    print("=== ClinVar Download and Conversion ===")
    results = download_and_convert_vcf(
        url="https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/",
        pattern=r"clinvar\.vcf\.gz$",
        name="clinvar",
        dest_dir=Path("/home/antonkulaga/data/download")
    )
    
    print("ClinVar download and conversion completed!")
    print(f"Results: {list(results.keys())}")
    
    # Print parquet paths
    if "vcf_parquet_path" in results:
        parquet_result = results["vcf_parquet_path"]
        parquet_out = parquet_result.output
        
        # Extract parquet paths
        if hasattr(parquet_out, "ravel"):
            parquet_seq = parquet_out.ravel().tolist()
        elif isinstance(parquet_out, list):
            parquet_seq = parquet_out
        else:
            parquet_seq = [parquet_out]
        
        parquet_paths = [p if isinstance(p, Path) else Path(p) for p in parquet_seq]
        
        print(f"\nGenerated {len(parquet_paths)} parquet file(s):")
        for parquet_path in parquet_paths:
            print(f"  {parquet_path}")
    
    print("\n" + "="*50)
    
    # Example 2: ClinVar download, conversion, AND variant splitting
    print("=== ClinVar Download, Conversion, and Variant Splitting ===")
    split_results = download_convert_and_split_vcf(
        url="https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/",
        pattern=r"clinvar\.vcf\.gz$",
        name="clinvar_split",
        dest_dir=Path("/home/antonkulaga/data/download"),
        explode_snv_alt=True
    )
    
    print("ClinVar download, conversion, and splitting completed!")
    print(f"Split results: {list(split_results.keys())}")
    
    # Print split variant paths
    if "split_variants_dict" in split_results:
        split_result = split_results["split_variants_dict"]
        split_out = split_result.output
        
        # Extract split variant dictionaries
        if hasattr(split_out, "ravel"):
            split_seq = split_out.ravel().tolist()
        elif isinstance(split_out, list):
            split_seq = split_out
        else:
            split_seq = [split_out]
        
        print(f"\nGenerated split variants for {len(split_seq)} file(s):")
        for i, split_dict in enumerate(split_seq):
            if isinstance(split_dict, dict):
                print(f"  File {i+1}:")
                for tsa, path in split_dict.items():
                    print(f"    {tsa}: {path}")
            else:
                print(f"  File {i+1}: {split_dict}")
