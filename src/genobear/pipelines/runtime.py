from __future__ import annotations

import os
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Optional

from eliot import start_action
from dotenv import find_dotenv, load_dotenv
from pipefunc import Pipeline

from genobear.config import get_default_workers, get_parquet_workers


def resolve_worker_counts(
    download_workers: Optional[int] = None,
    workers: Optional[int] = None,
    parquet_workers: Optional[int] = None,
) -> tuple[int, int, int]:
    """Resolve worker counts from parameters or environment.

    - download_workers: from GENOBEAR_DOWNLOAD_WORKERS or CPU count
    - workers: from GENOBEAR_WORKERS via get_default_workers()
    - parquet_workers: from GENOBEAR_PARQUET_WORKERS or default of 4
    """
    # Load .env if present (does not override existing env vars)
    env_path = find_dotenv(usecwd=True)
    if env_path:
        with start_action(action_type="load_env", env_path=env_path):
            load_dotenv(env_path, override=False)

    env_dl = os.getenv("GENOBEAR_DOWNLOAD_WORKERS")
    env_workers = os.getenv("GENOBEAR_WORKERS")
    env_parquet = os.getenv("GENOBEAR_PARQUET_WORKERS")

    resolved_download = (
        int(os.getenv("GENOBEAR_DOWNLOAD_WORKERS", os.cpu_count() or 1))
        if download_workers is None
        else max(1, int(download_workers))
    )
    resolved_workers = get_default_workers() if workers is None else max(1, int(workers))
    resolved_parquet = get_parquet_workers() if parquet_workers is None else max(1, int(parquet_workers))

    with start_action(
        action_type="resolve_worker_counts",
        GENOBEAR_DOWNLOAD_WORKERS=env_dl,
        GENOBEAR_WORKERS=env_workers,
        GENOBEAR_PARQUET_WORKERS=env_parquet,
        resolved_download=resolved_download,
        resolved_workers=resolved_workers,
        resolved_parquet=resolved_parquet,
    ):
        pass
    return resolved_download, resolved_workers, resolved_parquet


@contextmanager
def default_executors(
    download_workers: Optional[int] = None,
    workers: Optional[int] = None,
    parquet_workers: Optional[int] = None,
):
    """Context manager that yields a pipefunc executors mapping.

    Uses ThreadPool for download stages and ProcessPool for parquet and other CPU-bound tasks.
    Keys:
    - "vcf_local": ThreadPoolExecutor(download_workers) - for I/O-bound downloads
    - "vcf_parquet_path": ProcessPoolExecutor(parquet_workers) - for parquet conversion
    - "": default fallback ProcessPoolExecutor(parquet_workers) - for other tasks including splitting
    
    Note: We use parquet_workers as the default to ensure memory-intensive operations like
    splitting also use the conservative worker count. The split_variants_dict output uses
    this default executor when splitting is enabled.
    """
    dl_workers, cpu_workers, parq_workers = resolve_worker_counts(
        download_workers=download_workers, 
        workers=workers,
        parquet_workers=parquet_workers
    )
    # Use parquet_workers for default to ensure all parquet operations use conservative count
    default_exec = ProcessPoolExecutor(max_workers=parq_workers)
    # Use threads for I/O-bound download stage
    download_exec = ThreadPoolExecutor(max_workers=dl_workers)
    # Use separate pool with conservative worker count for memory-intensive parquet operations
    parquet_exec = ProcessPoolExecutor(max_workers=parq_workers)
    # Log configured and actual executor worker counts
    with start_action(
        action_type="init_executors",
        dl_workers=dl_workers,
        workers=cpu_workers,
        parquet_workers=parq_workers,
    ) as action:
        action.log(
            message_type="executors_created",
            download_executor="ThreadPoolExecutor",
            download_max_workers=getattr(download_exec, "_max_workers", None),
            parquet_executor="ProcessPoolExecutor",
            parquet_max_workers=getattr(parquet_exec, "_max_workers", None),
            default_executor="ProcessPoolExecutor",
            default_max_workers=getattr(default_exec, "_max_workers", None),
        )
    try:
        yield {
            "vcf_local": download_exec,
            "vcf_parquet_path": parquet_exec,
            "": default_exec,  # Default for all other outputs (including split_variants_dict when present)
        }
    finally:
        # Ensure clean shutdown
        download_exec.shutdown(wait=True, cancel_futures=False)
        parquet_exec.shutdown(wait=True, cancel_futures=False)
        default_exec.shutdown(wait=True, cancel_futures=False)


def run_pipeline(
    pipeline: Pipeline,
    inputs: dict,
    output_names: Optional[set[str]] = None,
    run_folder: Optional[str | Path] = None,
    return_results: bool = True,
    show_progress: str | bool = "rich",
    parallel: Optional[bool] = None,
    download_workers: Optional[int] = None,
    workers: Optional[int] = None,
    parquet_workers: Optional[int] = None,
    executors: Optional[dict] = None,
):
    """Execute a pipefunc Pipeline with GENOBEAR executors configuration."""
    dl_workers, cpu_workers, parq_workers = resolve_worker_counts(
        download_workers=download_workers,
        workers=workers,
        parquet_workers=parquet_workers
    )

    if executors is None:
        with default_executors(
            download_workers=dl_workers,
            workers=cpu_workers,
            parquet_workers=parq_workers
        ) as exec_map:
            with start_action(
                action_type="execute_pipeline",
                run_folder=str(run_folder) if run_folder else None,
                dl_workers=dl_workers,
                workers=cpu_workers,
                parquet_workers=parq_workers
            ):
                return pipeline.map(
                    inputs=inputs,
                    output_names=output_names,
                    parallel=True if parallel is None else parallel,
                    return_results=return_results,
                    show_progress=show_progress,
                    run_folder=run_folder,
                    executor=exec_map,
                )
    else:
        with start_action(
            action_type="execute_pipeline",
            run_folder=str(run_folder) if run_folder else None,
            dl_workers=dl_workers,
            workers=cpu_workers,
            parquet_workers=parq_workers
        ):
            return pipeline.map(
                inputs=inputs,
                output_names=output_names,
                parallel=True if parallel is None else parallel,
                return_results=return_results,
                show_progress=show_progress,
                run_folder=run_folder,
                executor=executors,
            )


