"""Download ensembl_variations dataset from HuggingFace Hub to local cache."""

import os
from pathlib import Path
from typing import Optional

from eliot import start_action
from huggingface_hub import snapshot_download
from platformdirs import user_cache_dir
from pipefunc import pipefunc


@pipefunc(output_name="ensembl_cache_path", cache=True)
def download_ensembl_from_hf(
    repo_id: str = "just-dna-seq/ensembl_variations",
    cache_dir: Optional[Path] = None,
    token: Optional[str] = None,
    force_download: bool = False,
    allow_patterns: Optional[list[str]] = None,
) -> Path:
    """
    Download ensembl_variations dataset from HuggingFace Hub to local cache.
    
    If the cache directory already exists and force_download is False, the download is skipped.
    
    Args:
        repo_id: HuggingFace repository ID (default: just-dna-seq/ensembl_variations)
        cache_dir: Local cache directory. If None, uses ~/.cache/genobear/ensembl_variations/splitted_variants
        token: HuggingFace API token. If None, uses HF_TOKEN env variable or public access
        force_download: If True, download even if cache exists
        allow_patterns: List of glob patterns to filter files to download (e.g., ["data/SNV/*.parquet"])
        
    Returns:
        Path to the local cache directory containing downloaded parquet files
        
    Example:
        >>> cache_path = download_ensembl_from_hf()
        >>> print(f"Downloaded to: {cache_path}")
        >>> # Skip download if already exists
        >>> cache_path = download_ensembl_from_hf(force_download=False)
    """
    with start_action(
        action_type="download_ensembl_from_hf",
        repo_id=repo_id,
        force_download=force_download
    ) as action:
        # Determine cache directory
        if cache_dir is None:
            # Check for environment variable override
            env_cache = os.getenv("GENOBEAR_CACHE_DIR")
            if env_cache:
                cache_dir = Path(env_cache) / "ensembl_variations" / "splitted_variants"
            else:
                user_cache_path = Path(user_cache_dir(appname="genobear"))
                cache_dir = user_cache_path / "ensembl_variations" / "splitted_variants"
        else:
            cache_dir = Path(cache_dir)
        
        action.log(
            message_type="info",
            step="cache_dir_determined",
            cache_dir=str(cache_dir)
        )
        
        # Check if cache exists and skip download if not forced
        if cache_dir.exists() and not force_download:
            # Check if there are any parquet files
            parquet_files = list(cache_dir.rglob("*.parquet"))
            if parquet_files:
                action.log(
                    message_type="info",
                    step="cache_exists_skip_download",
                    num_files=len(parquet_files)
                )
                return cache_dir
        
        action.log(
            message_type="info",
            step="downloading_from_hf",
            repo_id=repo_id
        )
        
        # Download from HuggingFace Hub
        # snapshot_download returns the path to the downloaded snapshot
        # We specify local_dir to control where files are extracted
        downloaded_path = snapshot_download(
            repo_id=repo_id,
            repo_type="dataset",
            local_dir=cache_dir,
            local_dir_use_symlinks=False,  # Copy files instead of symlinks for reliability
            token=token,
            allow_patterns=allow_patterns or ["data/**/*.parquet"],  # Only download parquet files from data folder
        )
        
        action.log(
            message_type="info",
            step="download_complete",
            downloaded_path=str(downloaded_path)
        )
        
        return Path(downloaded_path)


@pipefunc(output_name="ensembl_cache_path_checked")
def ensure_ensembl_cache(
    cache_dir: Optional[Path] = None,
    repo_id: str = "just-dna-seq/ensembl_variations",
    token: Optional[str] = None,
    force_download: bool = False,
) -> Path:
    """
    Ensure ensembl_variations cache exists, downloading if necessary.
    
    This is a convenience function that checks if the cache exists and downloads
    only if needed (unless force_download is True).
    
    Args:
        cache_dir: Local cache directory. If None, uses ~/.cache/genobear/ensembl_variations/splitted_variants
        repo_id: HuggingFace repository ID
        token: HuggingFace API token
        force_download: If True, force re-download even if cache exists
        
    Returns:
        Path to the local cache directory
    """
    return download_ensembl_from_hf(
        repo_id=repo_id,
        cache_dir=cache_dir,
        token=token,
        force_download=force_download,
    )

