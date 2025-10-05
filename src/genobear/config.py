import os


def get_default_workers() -> int:
    """
    Return default workers from GENOBEAR_WORKERS env var or CPU count.

    Downloaders that manage their own workers should not use this.
    """
    return int(os.getenv("GENOBEAR_WORKERS", os.cpu_count() or 1))


def get_parquet_workers() -> int:
    """
    Return parquet workers from GENOBEAR_PARQUET_WORKERS env var or default of 4.

    This is used for memory-intensive parquet operations (conversion, splitting, etc.) to avoid memory overload.
    Default is 4 to balance performance and memory usage.
    """
    return int(os.getenv("GENOBEAR_PARQUET_WORKERS", 4))


def get_download_workers() -> int:
    """
    Return download workers from GENOBEAR_DOWNLOAD_WORKERS env var or CPU count.

    Used for parallel I/O-bound download operations.
    """
    return int(os.getenv("GENOBEAR_DOWNLOAD_WORKERS", os.cpu_count() or 1))


