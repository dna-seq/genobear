import os
import re
import shutil
import time
from pathlib import Path
from typing import List, Optional

import aiohttp
import fsspec
import polars as pl
from aiohttp import ClientResponseError, ClientTimeout
from eliot import start_action
from fsspec.exceptions import FSTimeoutError
from pipefunc import Pipeline, pipefunc
from pipefunc.typing import Array
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
    # mmap cache errors are retryable (blockcache corruption)
    if isinstance(exc, ValueError) and "mmap length is greater than file size" in str(exc):
        return True
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

    # Use filecache instead of blockcache to avoid mmap issues with concurrent/parallel downloads
    # blockcache can fail with "mmap length is greater than file size" when cache files are being created
    # For now, always use filecache as it's more reliable for parallel downloads
    cache_proto = "filecache"
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

    def _clear_cache_for_url() -> None:
        """Clear any cached files for this specific URL."""
        import hashlib
        # fsspec uses the URL to generate cache filenames
        url_hash = hashlib.md5(url.encode()).hexdigest()
        # Try to find and remove any cache files related to this URL
        for pattern in [f"*{url_hash}*", f"*{url.rsplit('/', 1)[-1]}*"]:
            cache_files = list(cache_storage.glob(pattern))
            for cf in cache_files:
                try:
                    if cf.is_file():
                        cf.unlink()
                except Exception:
                    pass

    def _download_to_tmp_with_retry(destination_tmp: Path) -> None:
        retryer = Retrying(
            retry=retry_if_exception(_retryable_http_error),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=30.0),
            stop=stop_after_attempt(retries),
            reraise=True,
        )

        if destination_tmp.exists():
            destination_tmp.unlink()

        # Get progress logging interval from env (default 10 seconds)
        log_interval = float(os.getenv("GENOBEAR_PROGRESS_INTERVAL", "10.0"))

        for attempt in retryer:
            with attempt:
                with fsspec.open(chained_url, mode="rb", **storage_options) as src, open(destination_tmp, "wb") as dst:
                    # Progress tracking with rate limiting
                    start_time = time.time()
                    last_log_time = start_time
                    total_bytes = 0
                    
                    while True:
                        data = src.read(chunk_size)
                        if not data:
                            break
                        dst.write(data)
                        total_bytes += len(data)
                        
                        # Rate-limited progress logging
                        current_time = time.time()
                        if current_time - last_log_time >= log_interval:
                            mb_downloaded = total_bytes / (1024 * 1024)
                            elapsed = current_time - start_time
                            speed_mbps = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                            
                            action.log(
                                message_type="download_progress",
                                url=url,
                                mb_downloaded=round(mb_downloaded, 2),
                                speed_mbps=round(speed_mbps, 2),
                                elapsed_seconds=round(elapsed, 1)
                            )
                            last_log_time = current_time
                    
                    # Log 100% completion with final stats
                    if total_bytes > 0:
                        mb_final = total_bytes / (1024 * 1024)
                        total_elapsed = time.time() - start_time
                        avg_speed = mb_final / total_elapsed if total_elapsed > 0 else 0
                        
                        action.log(
                            message_type="download_complete",
                            url=url,
                            mb_total=round(mb_final, 2),
                            total_seconds=round(total_elapsed, 1),
                            avg_speed_mbps=round(avg_speed, 2),
                            progress_percent=100
                        )

    action_kwargs = {"action_type": "download_path", "url": url, "dest": str(local)}
    if dest_dir_was_provided:
        action_kwargs["dest_dir"] = str(dest_dir)

    with start_action(**action_kwargs) as action:
        action.log(message_type="info", step="start_download", url=url, timeout=timeout)

        # If the final file already exists with correct size, skip download
        if local.exists() and not check_files:
            action.log(message_type="info", step="skip_existing_file", path=str(local))
            return local

        # Clear any corrupted cache files before attempting download
        _clear_cache_for_url()

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


@pipefunc(
    output_name=("validated_urls", "validated_vcf_local", "validated_vcf_parquet_path"),
    renames={
        "urls": "urls",
        "vcf_local": "vcf_local",
        "vcf_parquet_path": "vcf_parquet_path",
    },
)
def validate_downloads_and_parquet(
    urls: list[str],
    vcf_local: Array[Path],
    vcf_parquet_path: Array[Path],
) -> tuple[list[str], list[Path], list[Path]]:
    """
    Reduce-style validator to ensure that for every discovered URL we have a
    downloaded local file, and for every downloaded VCF we produced a parquet.

    Also validates the file sizes of downloaded files against expected sizes if available.

    Returns the validated lists to keep them available to downstream steps.
    """
    with start_action(action_type="validate_downloads_and_parquet") as action:
        # Normalize singletons to lists
        if isinstance(vcf_local, Path):
            vcf_local_list = [vcf_local]
        else:
            vcf_local_list = list(vcf_local)

        if isinstance(vcf_parquet_path, Path):
            parquet_list = [vcf_parquet_path]
        else:
            parquet_list = list(vcf_parquet_path)

        # Filter out non-VCF entries from conversion stage (e.g., .tbi/.csi) which return empty LazyFrame and original path
        # We consider a file a VCF if name contains .vcf and is not an index
        def _is_vcf(p: Path) -> bool:
            suffixes = p.suffixes
            if not suffixes:
                return False
            is_index = suffixes and suffixes[-1] in [".tbi", ".csi", ".idx"]
            return (".vcf" in suffixes) and (not is_index)

        vcf_files = [p for p in vcf_local_list if _is_vcf(p)]

        # Basic existence checks
        missing_locals = [p for p in vcf_local_list if not Path(p).exists()]
        if missing_locals:
            action.log(message_type="error", missing_locals=[str(p) for p in missing_locals])
            raise FileNotFoundError(f"Missing downloaded files: {missing_locals}")

        # Ensure each URL has a corresponding local file by filename
        local_by_name = {Path(p).name: Path(p) for p in vcf_local_list}
        missing_for_urls: list[tuple[str, str]] = []  # (url, expected_filename)
        for url in urls:
            expected_name = url.rsplit("/", 1)[-1]
            if expected_name not in local_by_name:
                missing_for_urls.append((url, expected_name))
        if missing_for_urls:
            action.log(
                message_type="error",
                missing_by_url=[{"url": u, "expected": n} for (u, n) in missing_for_urls],
            )
            raise FileNotFoundError(
                f"No local files matching URLs: {[n for (_, n) in missing_for_urls]}"
            )

        missing_parquet = [p for p in parquet_list if not Path(p).exists()]
        # parquet_list may be empty if all inputs were non-VCF (e.g., only indexes matched); that's acceptable.
        if parquet_list and missing_parquet:
            action.log(message_type="error", missing_parquet=[str(p) for p in missing_parquet])
            raise FileNotFoundError(f"Missing parquet files: {missing_parquet}")

        # Heuristic: number of parquet files should be >= number of VCF files converted
        # (some steps may legitimately skip or deduplicate; we do a soft check/logging)
        if parquet_list and len(parquet_list) < len(vcf_files):
            action.log(
                message_type="warning",
                reason="fewer_parquets_than_vcfs",
                vcfs=len(vcf_files),
                parquets=len(parquet_list),
            )

        # Validate file sizes using fsspec if possible, matched by filename
        size_mismatches = []
        for url in urls:
            expected_name = url.rsplit("/", 1)[-1]
            local_path = local_by_name.get(expected_name)
            if local_path is None:
                # Already handled above, continue just in case
                continue
            try:
                fs, path = fsspec.core.url_to_fs(url)
                remote_size = None
                try:
                    info = fs.info(path)
                    remote_size = info.get("size") if isinstance(info, dict) else None
                except Exception:
                    # Fallback to fs.size if available
                    if hasattr(fs, "size"):
                        remote_size = fs.size(path)
                if remote_size is not None:
                    local_size = local_path.stat().st_size
                    if local_size != remote_size:
                        size_mismatches.append(
                            {
                                "url": url,
                                "local_path": str(local_path),
                                "local_size": int(local_size),
                                "remote_size": int(remote_size),
                            }
                        )
                else:
                    action.log(
                        message_type="warning",
                        reason="remote_size_unavailable",
                        url=url,
                        path=str(path),
                    )
            except Exception as e:
                action.log(message_type="warning", reason="size_check_failed", url=url, error=str(e))

        if size_mismatches:
            action.log(message_type="error", size_mismatches=size_mismatches)
            def _fmt_size(n: int) -> str:
                units = ["B", "KB", "MB", "GB", "TB"]
                size = float(n)
                for unit in units:
                    if size < 1024.0 or unit == units[-1]:
                        return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
                    size /= 1024.0
                return f"{int(n)} B"
            lines = []
            for m in size_mismatches:
                diff = int(m["local_size"]) - int(m["remote_size"])
                sign = "+" if diff > 0 else ""
                lines.append(
                    f"{m['local_path']}: local {_fmt_size(int(m['local_size']))} vs remote {_fmt_size(int(m['remote_size']))} ({sign}{diff} B)"
                )
            message = "File size mismatches (" + str(len(size_mismatches)) + "):\n" + "\n".join(lines)
            raise ValueError(message)

        action.log(
            message_type="info",
            urls_count=len(urls),
            local_count=len(vcf_local_list),
            parquet_count=len(parquet_list),
        )

        # Return validated outputs to keep pipeline continuity
        return urls, vcf_local_list, parquet_list


def make_vcf_pipeline() -> Pipeline:
    """Create the base VCF download + convert + validate pipeline."""
    return Pipeline(
        [
            list_paths,
            download_path,
            convert_to_parquet,
            validate_downloads_and_parquet,
        ],
        print_error=True,
    )

def make_validation_pipeline() -> Pipeline:
    """Create a pipeline focused solely on validating already downloaded VCF and parquet files."""
    return Pipeline(
        [
            validate_downloads_and_parquet,
        ],
        # Keep errors concise; avoid verbose pipefunc error snapshots that list all inputs
        print_error=False,
    )

if __name__ == "__main__":
    from genobear import PreparationPipelines
    
    print("Running validation for Ensembl downloads...")
    validation_results = PreparationPipelines.validate_ensembl()
    print("Validation completed. Results:", validation_results)