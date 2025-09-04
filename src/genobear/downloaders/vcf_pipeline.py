import re
import shutil
from pathlib import Path
from typing import List, Optional

import fsspec
from eliot import start_action
from pipefunc import pipefunc, Pipeline
from platformdirs import user_cache_dir
from aiohttp import ClientTimeout  # optional, only if the HTTP FS supports it
from genobear.io import vcf_to_parquet, AnnotatedLazyFrame

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
    timeout_s: float | None = 300.0,
) -> Path:
    """Download a single remote path using fsspec with filecache."""
    # Resolve defaults
    user_cache_path = Path(user_cache_dir(appname="genobear"))
    
    if dest_dir is None:
        if isinstance(name, str):
            dest_dir = user_cache_path / name
        else:
            dest_dir = name
    if cache_storage is None:
        # If dest_dir is inside user cache, use it as cache storage for efficiency
        if dest_dir.resolve().is_relative_to(user_cache_path.resolve()):
            cache_storage = dest_dir
        else:
            # dest_dir is outside user cache, use separate cache directory
            cache_storage = user_cache_path / ".fsspec_cache"

    dest_dir.mkdir(parents=True, exist_ok=True)
    cache_storage = Path(cache_storage)
    cache_storage.mkdir(parents=True, exist_ok=True)

    # Final output path in dest_dir
    local = dest_dir / url.rsplit("/", 1)[-1]

    # Chained URL to activate filecache
    chained_url = f"filecache::{url}"

    # Build storage options correctly for open_local:
    # - filecache options go under 'filecache'
    # - http/https options go under 'http'/'https' as needed
    storage_options = {
        "filecache": {
            "cache_storage": str(cache_storage),
            "check_files": check_files,
            "expiry_time": expiry_time,
        }
    }
    # If the URL is HTTPS/HTTP, pass client options under the protocol key
    if url.startswith("https://"):
        storage_options["https"] = {"client_kwargs": {"timeout": ClientTimeout(total=timeout_s)}} if timeout_s is not None else {}
    elif url.startswith("http://"):
        storage_options["http"] = {"client_kwargs": {"timeout": ClientTimeout(total=timeout_s)}} if timeout_s is not None else {}

    with start_action(action_type="download_path", url=url, dest=str(local)):
        # Resolve the cached local path; downloads only on miss/expiry/check failure
        cached_local_path = fsspec.open_local(
            chained_url,
            mode="rb",
            **storage_options
        )
        cached_path = Path(cached_local_path)

        # If dest_dir equals the cache location, return the cached file directly
        if cached_path.resolve().parent == dest_dir.resolve():
            return cached_path

        # Copy from cache to dest if missing or size differs
        if not local.exists() or local.stat().st_size != cached_path.stat().st_size:
            shutil.copy2(cached_path, local)

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

def make_clinvar_pipeline() -> Pipeline:
    """Create a pipeline with ClinVar-specific defaults."""
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
    
    return pipeline.with_defaults(**clinvar_defaults)

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

if __name__ == "__main__":
    # Simple ClinVar download example
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
