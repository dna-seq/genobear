from pathlib import Path
from typing import Union, Optional, Literal, Tuple, TypeVar
import polars as pl
import polars_bio as pb
from eliot import start_action
import os
import pooch

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


def _default_parquet_path(vcf_path: Path) -> Path:
    """Generate default parquet path next to VCF file."""
    # Remove .vcf or .vcf.gz extension before adding .parquet
    if vcf_path.suffixes == ['.vcf', '.gz']:
        # Handle .vcf.gz files
        return vcf_path.with_suffix('').with_suffix('.parquet')
    elif vcf_path.suffix == '.vcf':
        # Handle .vcf files
        return vcf_path.with_suffix('.parquet')
    else:
        # Fallback for other file types
        return vcf_path.with_suffix('.parquet')


def resolve_genobear_subfolder(subdir_name: str, base: Optional[Union[str, Path]] = None) -> Path:
    """
    Resolve a subfolder path using the same mechanism as MultiVCFDownloader.
    
    This function provides a consistent way to resolve data storage paths across
    the genobear package, respecting the GENOBEAR_FOLDER environment variable.
    
    Args:
        subdir_name: Name of the subdirectory to create/use
        base: Base directory path. If None, uses GENOBEAR_FOLDER env var or defaults to "genobear"
        
    Returns:
        Absolute path to the resolved subfolder
        
    Examples:
        >>> # Using default base (GENOBEAR_FOLDER env var or "genobear")
        >>> resolve_genobear_subfolder("downloads")
        PosixPath('/home/user/.cache/genobear/downloads')
        
        >>> # Using custom base
        >>> resolve_genobear_subfolder("vcf_files", "/tmp/myproject")
        PosixPath('/tmp/myproject/vcf_files')
    """
    if base is None:
        base = os.getenv("GENOBEAR_FOLDER", "genobear")
    
    base_path = Path(base)
    
    # If it's just a name (no path separators), use OS cache
    if len(base_path.parts) == 1 and not base_path.is_absolute():
        cache_path = Path(pooch.os_cache(str(base_path))) / subdir_name
    else:
        cache_path = base_path / subdir_name
        
    return cache_path.resolve()


def get_info_fields(vcf_path: str) -> list[str]:
    """
    Extract INFO field names from a VCF file header by parsing the header directly.
    
    Args:
        vcf_path: Path to the VCF file
        
    Returns:
        List of INFO field names found in the header
    """
    with start_action(action_type="get_info_fields", vcf_path=vcf_path) as action:
        try:
            import gzip
            info_fields = []
            
            # Determine if file is gzipped
            open_func = gzip.open if vcf_path.endswith('.gz') else open
            mode = 'rt' if vcf_path.endswith('.gz') else 'r'
            
            with open_func(vcf_path, mode) as f:
                for line in f:
                    line = line.strip()
                    # Stop when we reach the data (non-header lines)
                    if not line.startswith('#'):
                        break
                    # Look for INFO field definitions
                    if line.startswith('##INFO=<ID='):
                        # Extract the ID from ##INFO=<ID=FIELD_NAME,...>
                        start_idx = line.find('ID=') + 3
                        end_idx = line.find(',', start_idx)
                        if end_idx == -1:  # In case there's no comma after ID
                            end_idx = line.find('>', start_idx)
                        if end_idx > start_idx:
                            field_name = line[start_idx:end_idx]
                            info_fields.append(field_name)
            
            action.log(
                message_type="info",
                step="info_fields_extracted",
                count=len(info_fields),
                fields=info_fields
            )
            return info_fields
            
        except Exception as e:
            action.log(
                message_type="error",
                step="info_fields_extraction_failed",
                error=str(e)
            )
            # Return empty list if extraction fails
            return []


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
    save_parquet: SaveParquet = "auto"
) -> pl.LazyFrame:
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
        save_parquet: Controls saving to parquet.
            - None: do not save
            - "auto": save next to the input, replacing .vcf/.vcf.gz with .parquet
            - Path: save to the provided location

    Returns:
        Polars LazyFrame or DataFrame containing the VCF data.
        When saving to parquet, returns a LazyFrame that reads from the parquet file
        (preserving lazy evaluation while ensuring temp files are cleaned up).
        When save_parquet=None, returns the original LazyFrame from polars-bio.

    Note:
        VCF reader uses **1-based** coordinate system for the `start` and `end` columns.
        When saving to parquet, the function creates the parquet file next to the original VCF
        (for "auto") or at the provided path.
    """
    with start_action(
        action_type="read_vcf_file",
        file_path=str(file_path),
        save_parquet=str(save_parquet) if save_parquet else None
    ) as action:
        file_path = Path(file_path)

        # Prepare kwargs for pb.scan_vcf using locals(), excluding file_path and save_parquet
        vcf_kwargs = {k: v for k, v in locals().items() if k not in ("file_path", "save_parquet", "action")}

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

        # Let polars-bio handle compression autodetection and any VCF format issues
        vcf_kwargs["info_fields"] = get_info_fields(str(file_path)) if info_fields is None else info_fields

        result = pb.scan_vcf(str(file_path), **vcf_kwargs)

        action.log(
            message_type="info",
            step="vcf_read_complete",
            result_type=type(result).__name__,
            rows=result.height if hasattr(result, 'height') else 'unknown'
        )

        # Handle parquet saving if requested
        if parquet_path:
            with start_action(
                action_type="save_parquet",
                parquet_path=str(parquet_path)
            ) as save_action:
                if isinstance(result, pl.LazyFrame):
                    # Force collection to avoid streaming issues
                    result.collect(streaming=False).write_parquet(str(parquet_path))
                else:
                    result.write_parquet(str(parquet_path))
                
                save_action.log(
                    message_type="info",
                    step="parquet_saved",
                    parquet_path=str(parquet_path)
                )
                
                # Return a LazyFrame that reads from the parquet file
                return pl.scan_parquet(str(parquet_path))
        
        # Return the original result if no parquet saving
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
            save_parquet=output_path
        )
        
        action.log(
            message_type="info",
            step="conversion_complete",
            output_path=str(output_path)
        )
        
        return lazy_frame, output_path
    






