import gzip
from pathlib import Path
import tempfile
from typing import Union, Optional, Literal, Tuple, TypeVar, Generic
import polars as pl
import polars_bio as pb
import polars.exceptions as ple
from eliot import start_action

# Type variable for generic data types
T = TypeVar('T')

# Generic type for results that include both data and the file path where it's saved
AnnotatedResult = Tuple[T, Path]

# Specific type alias for LazyFrame results with their parquet paths
AnnotatedLazyFrame = AnnotatedResult[pl.LazyFrame]

# Type alias describing how parquet saving is configured:
# - None: do not save
# - "auto": save next to the input, replacing .vcf/.vcf.gz with .parquet
# - Path: save to the provided absolute/relative path
SaveParquet = Union[Path, Literal["auto"], None]

def _default_parquet_path(src: Path) -> Path:
    """Compute a default parquet path for a given VCF/VCF.GZ path.

    Replaces .vcf or .vcf.gz (case-insensitive) with .parquet in the same directory.
    """
    name_lower = src.name.lower()
    if name_lower.endswith(".vcf.gz"):
        base = src.name[:-7]  # drop '.vcf.gz'
        return src.with_name(f"{base}.parquet")
    if name_lower.endswith(".vcf"):
        base = src.name[:-4]  # drop '.vcf'
        return src.with_name(f"{base}.parquet")
    # Fallback: just append .parquet to the stem
    return src.with_suffix(".parquet")

def get_info_fields(file_path: Union[str, Path]) -> list[str]:
    file_path_str = str(Path(file_path))
    return pb.describe_vcf(file_path_str).select("name").to_series().to_list()

def read_vcf_file(
    file_path: Union[str, Path],
    info_fields: Union[list[str], None] = None,
    thread_num: int = 1,
    chunk_size: int = 8,
    concurrent_fetches: int = 1,
    allow_anonymous: bool = True,
    enable_request_payer: bool = False,
    max_retries: int = 5,
    timeout: int = 300,
    compression_type: str = "auto",
    streaming: bool = False,
    save_parquet: SaveParquet = "auto",
    return_lazy: bool = True
) -> Union[pl.LazyFrame, pl.DataFrame]:
    """
    Read a VCF file using polars-bio, automatically handling gzipped files.
    
    Args:
        file_path: Path to the VCF file (can be .vcf or .vcf.gz)
        info_fields: The fields to read from the INFO column.
        thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
        chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
        concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
        allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
        enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
        max_retries: The maximum number of retries for reading the file from object storage.
        timeout: The timeout in seconds for reading the file from object storage.
        compression_type: The compression type of the VCF file. If not specified, it will be detected automatically based on the file extension. BGZF compression is supported ('bgz').
        streaming: Whether to read the VCF file in streaming mode. Note: Streaming mode may not be fully supported by polars-bio and can cause 'not implemented' errors.
        save_parquet: Controls saving to parquet.
            - None: do not save
            - "auto": save next to the input, replacing .vcf/.vcf.gz with .parquet
            - Path: save to the provided location
        return_lazy: Whether to return a LazyFrame (True) or DataFrame (False). When True, returns LazyFrame for lazy evaluation.
        
    Returns:
        Polars LazyFrame or DataFrame containing the VCF data.
        When saving to parquet, returns a LazyFrame that reads from the parquet file 
        (preserving lazy evaluation while ensuring temp files are cleaned up).
        When return_lazy=True and save_parquet=None, returns the original LazyFrame from polars-bio.
        
    Note:
        VCF reader uses **1-based** coordinate system for the `start` and `end` columns.
        Streaming mode is experimental and may not work with all VCF files.
        When saving to parquet, the function creates the parquet file next to the original VCF 
        (for "auto") or at the provided path.
    """
    with start_action(
        action_type="read_vcf_file",
        file_path=str(file_path),
        streaming=streaming,
        save_parquet=str(save_parquet) if save_parquet else None,
        return_lazy=return_lazy
    ) as action:
        file_path = Path(file_path)

        # Prepare kwargs for pb.read_vcf using locals(), excluding file_path, save_parquet, and return_lazy
        vcf_kwargs = {k: v for k, v in locals().items() if k not in ("file_path", "save_parquet", "return_lazy", "action")}

        # Resolve parquet path decision early
        if isinstance(save_parquet, Path):
            parquet_path: Optional[Path] = save_parquet
        elif save_parquet == "auto":
            parquet_path = _default_parquet_path(file_path)
        else:
            parquet_path = None

        action.log(
            message_type="info",
            step="reading_vcf",
            parquet_path=str(parquet_path) if parquet_path else None
        )

        # Always let polars-bio handle compression autodetection
        vcf_kwargs["info_fields"] = get_info_fields(str(file_path)) if info_fields is None else info_fields
        result = pb.read_vcf(str(file_path), **vcf_kwargs)

        action.log(
            message_type="info",
            step="vcf_read_complete",
            result_type=type(result).__name__
        )

        if parquet_path is not None:
            with start_action(action_type="save_to_parquet", parquet_path=str(parquet_path)) as parquet_action:
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                
                if isinstance(result, pl.LazyFrame):
                    result.collect(streaming=False).write_parquet(str(parquet_path))
                else:
                    result.write_parquet(str(parquet_path))
                
                parquet_action.log(message_type="info", step="parquet_saved")
            
            return pl.scan_parquet(str(parquet_path))

        # Return LazyFrame/DataFrame based on return_lazy preference
        if return_lazy and isinstance(result, pl.DataFrame):
            action.log(message_type="info", step="converting_to_lazy")
            return result.lazy()
        elif not return_lazy and isinstance(result, pl.LazyFrame):
            action.log(message_type="info", step="collecting_lazy_frame")
            return result.collect()
        
        return result

def vcf_to_parquet(
    vcf_path: Union[str, Path],
    parquet_path: Optional[Union[str, Path]] = None,
    info_fields: Union[list[str], None] = None,
    thread_num: int = 1,
    chunk_size: int = 8,
    concurrent_fetches: int = 1,
    allow_anonymous: bool = True,
    enable_request_payer: bool = False,
    max_retries: int = 5,
    timeout: int = 300,
    compression_type: str = "auto",
    streaming: bool = False,
    overwrite: bool = False
) -> AnnotatedLazyFrame:
    """
    Read a VCF file and save it to Parquet format, returning both the path and LazyFrame.
    
    This function is a convenience wrapper around read_vcf_file that focuses specifically
    on VCF to Parquet conversion, ensuring the output is always saved and returning
    both the path to the created Parquet file and a LazyFrame for immediate data access.
    
    Args:
        vcf_path: Path to the input VCF file (can be .vcf or .vcf.gz)
        parquet_path: Path where to save the Parquet file. If None, saves next to VCF with .parquet extension
        info_fields: The fields to read from the INFO column. If None, reads all available fields
        thread_num: Number of threads for parallel decompression (local files only)
        chunk_size: Chunk size in MB for object store reading (default 8MB)
        concurrent_fetches: Number of concurrent fetches for object store (default 1)
        allow_anonymous: Whether to allow anonymous access to object storage
        enable_request_payer: Whether to enable request payer for AWS S3
        max_retries: Maximum retries for object storage operations
        timeout: Timeout in seconds for object storage operations
        compression_type: Compression type detection ("auto", "bgz", etc.)
        streaming: Whether to use streaming mode (experimental)
        overwrite: Whether to overwrite existing Parquet file (default False)
        
    Returns:
        AnnotatedLazyFrame: A tuple containing:
            - LazyFrame reading from the Parquet file for immediate data access  
            - Path to the created Parquet file
        
    Raises:
        FileExistsError: If parquet_path exists and overwrite=False
        
    Example:
        >>> vcf_path = Path("data/variants.vcf.gz")
        >>> lazy_df, parquet_path = vcf_to_parquet(vcf_path)
        >>> print(f"Converted to: {parquet_path}")
        >>> print(f"Shape: {lazy_df.select(pl.len()).collect().item()}")
        
        >>> # Custom output location
        >>> df, custom_path = vcf_to_parquet(vcf_path, "output/my_variants.parquet")
        >>> # Immediate data access
        >>> variant_count = df.select(pl.len()).collect().item()
    """
    with start_action(
        action_type="vcf_to_parquet",
        vcf_path=str(vcf_path),
        parquet_path=str(parquet_path) if parquet_path else None,
        overwrite=overwrite
    ) as action:
        vcf_path = Path(vcf_path)
        
        # Determine output path
        if parquet_path is None:
            output_path = _default_parquet_path(vcf_path)
        else:
            output_path = Path(parquet_path)
        
        action.log(
            message_type="info",
            step="output_path_determined",
            output_path=str(output_path)
        )
        
        # If parquet already exists and overwrite is False, reuse it
        if output_path.exists() and not overwrite:
            action.log(
                message_type="info",
                step="reusing_existing_parquet",
                path=str(output_path)
            )
            return pl.scan_parquet(str(output_path)), output_path
        
        # Use read_vcf_file with forced parquet saving
        lazy_frame = read_vcf_file(
            file_path=vcf_path,
            info_fields=info_fields,
            thread_num=thread_num,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            streaming=streaming,
            save_parquet=output_path,
            return_lazy=True  # Ensure we get a LazyFrame
        )
        
        action.log(
            message_type="info",
            step="conversion_complete",
            output_path=str(output_path)
        )
        
        return lazy_frame, output_path
    