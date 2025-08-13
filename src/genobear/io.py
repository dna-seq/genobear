import gzip
import os
from pathlib import Path
import tempfile
import shutil
from typing import Union, Optional, Literal, Tuple, TypeVar, Generic
import polars as pl
import polars_bio as pb
import polars.exceptions as ple
from eliot import start_action
from flashtext2 import KeywordProcessor
import mmap
import re

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
    clean_semicolons: bool = False
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
        streaming: Whether to read the VCF file in streaming mode. Note: Streaming mode may not be fully supported by polars-bio and can cause 'not implemented' errors.
        save_parquet: Controls saving to parquet.
            - None: do not save
            - "auto": save next to the input, replacing .vcf/.vcf.gz with .parquet
            - Path: save to the provided location
        clean_semicolons: Whether to clean malformed semicolons (;;, ;:, ;;:) before processing (default False)
        
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
        save_parquet=str(save_parquet) if save_parquet else None
    ) as action:
        file_path = Path(file_path)

        # Prepare kwargs for pb.read_vcf using locals(), excluding file_path, save_parquet, clean_semicolons, and return_lazy
        vcf_kwargs = {k: v for k, v in locals().items() if k not in ("file_path", "save_parquet", "clean_semicolons", "return_lazy", "action")}

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
            parquet_path=str(parquet_path) if parquet_path else None,
            clean_semicolons=clean_semicolons
        )

        # Handle semicolon cleaning if requested
        vcf_file_to_process = file_path
        if clean_semicolons:
            with start_action(action_type="clean_extra_semicolons_step", vcf_path=str(file_path)) as clean_action:
                file_path = Path(file_path)
                
                # Map polars-bio compression types to our function's expected format
                clean_compression = None
                if compression_type in ("gz", "bgz"):
                    clean_compression = compression_type
                elif compression_type == "auto":
                    # Auto-detect compression from file extension
                    if file_path.name.lower().endswith('.gz'):
                        clean_compression = "gz"
                    else:
                        clean_compression = None
                
                # Always edit in-place now, whether compressed or not
                vcf_file_to_process = clean_extra_semicolons(
                    file_path, 
                    output_path=None,  # In-place editing
                    compression=clean_compression,
                    inplace_for_compressed=True  # Enable in-place editing for compressed files
                )
                clean_action.log(
                    message_type="info",
                    step="semicolons_cleaned_inplace", 
                    cleaned_path=str(vcf_file_to_process),
                    was_compressed=clean_compression is not None
                )

        # Always let polars-bio handle compression autodetection
        vcf_kwargs["info_fields"] = get_info_fields(str(vcf_file_to_process)) if info_fields is None else info_fields
        result = pb.read_vcf(str(vcf_file_to_process), **vcf_kwargs)

        action.log(
            message_type="info",
            step="vcf_read_complete",
            result_type=type(result).__name__
        )

        if parquet_path is not None:
            with start_action(action_type="save_to_parquet", parquet_path=str(parquet_path)) as parquet_action:
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                
                if isinstance(result, pl.LazyFrame):
                    # Respect configured streaming mode; forcing streaming may be unstable for some VCFs
                    result.collect(streaming=streaming).write_parquet(str(parquet_path))
                else:
                    result.write_parquet(str(parquet_path))
                
                parquet_action.log(message_type="info", step="parquet_saved")
            
            return pl.scan_parquet(str(parquet_path))
        
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
    overwrite: bool = False,
    clean_semicolons: bool = False
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
        clean_semicolons: Whether to clean malformed semicolons (;;, ;:, ;;:) before processing (default False)
        
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
        
        # Clean extra semicolons if requested  
        vcf_file_to_process = vcf_path
        if clean_semicolons:
            with start_action(action_type="clean_extra_semicolons_step", vcf_path=str(vcf_path)) as clean_action:
                # Map polars-bio compression types to our function's expected format
                clean_compression = None
                if compression_type in ("gz", "bgz"):
                    clean_compression = compression_type
                elif compression_type == "auto":
                    # Auto-detect compression from file extension
                    if vcf_path.name.lower().endswith('.gz'):
                        clean_compression = "gz"
                    else:
                        clean_compression = None
                
                # Always edit in-place now, whether compressed or not
                vcf_file_to_process = clean_extra_semicolons(
                    vcf_path, 
                    output_path=None,  # In-place editing
                    compression=clean_compression,
                    inplace_for_compressed=True  # Enable in-place editing for compressed files
                )
                clean_action.log(
                    message_type="info",
                    step="semicolons_cleaned_inplace", 
                    cleaned_path=str(vcf_file_to_process),
                    was_compressed=clean_compression is not None
                )
        
        # Use read_vcf_file with forced parquet saving
        lazy_frame = read_vcf_file(
            file_path=vcf_file_to_process,
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
            save_parquet=output_path
        )
        
        action.log(
            message_type="info",
            step="conversion_complete",
            output_path=str(output_path)
        )
        
        return lazy_frame, output_path
    


def clean_extra_semicolons(
    vcf_path: Path | str,
    output_path: Optional[Path | str] = None,
    chunk_size: int = 25 * 1024 * 1024,
    compression: Optional[str] = None,
    inplace_for_compressed: bool = False,
    use_keyword_processor: bool = False,
    encoding: str = "utf-8",
) -> Path:
    """
    Fix problematic VCF characters using flashtext2.
    Processes in-place if output_path is None, otherwise writes to output_path.
    Memory-efficient chunk-based streaming for large files.
    
    Args:
        vcf_path: Path to the input VCF file
        output_path: Path for output file. If None, modifies in-place
        chunk_size: Size of chunks for streaming processing (default 25MB)
        compression: Compression type - "gz", "bgz", or None for uncompressed
            - "gz": treat as gzip compressed
            - "bgz": treat as bgzip compressed (same as gz for reading)
            - None: treat as uncompressed
        inplace_for_compressed: If True, enables in-place editing for compressed files
            by decompressing, cleaning, and recompressing. If False, compressed files
            require an output_path.
        use_keyword_processor: If True, uses flashtext2 KeywordProcessor for replacements
            even in in-place mode (decodes lines to str then re-encodes). For a very small
            set of fixed byte patterns, the default byte-level replacement is typically
            faster and more memory efficient.
        encoding: Text encoding to use when decoding/encoding lines if use_keyword_processor=True
    
    Returns:
        Path to the processed file
    """

    vcf_path = Path(vcf_path)
    
    # Configure flashtext2
    kp: KeywordProcessor = KeywordProcessor(case_sensitive=True)
    replacements: dict[str, str] = {
        ';;': ';',
        ';:': ';',
        ';;:': ';',
    }
    for old, new in replacements.items():
        kp.add_keyword(old, new)
    
    # -------------------- Internal helpers --------------------
    def process_vcf_content(content: str) -> str:
        """Preserve header lines, replace only in data lines."""
        header_pattern = re.compile(r'^((?:^#.*\n)*)', re.MULTILINE)
        match = header_pattern.match(content)
        if match:
            header_section: str = match.group(1)
            data_section: str = content[len(header_section):]
            cleaned = kp.replace_keywords(data_section)
            # Collapse any remaining repeated semicolons and fix ';:' → ';'
            cleaned = re.sub(r";{2,}", ";", cleaned)
            cleaned = cleaned.replace(";:", ";")
            return header_section + cleaned
        else:
            cleaned = kp.replace_keywords(content)
            cleaned = re.sub(r";{2,}", ";", cleaned)
            cleaned = cleaned.replace(";:", ";")
            return cleaned

    def process_mixed_chunk(chunk: str) -> str:
        """Process a chunk that may contain both header and data lines."""
        header_pattern = re.compile(r'^((?:^#.*\n)*)', re.MULTILINE)
        match = header_pattern.match(chunk)
        if match:
            header_section: str = match.group(1)
            data_section: str = chunk[len(header_section):]
            return header_section + kp.replace_keywords(data_section) if data_section else header_section
        else:
            return kp.replace_keywords(chunk)

    def fix_true_inplace() -> Path:
        """TRUE in-place modification. For uncompressed files uses chunked temp file approach. For compressed files decompresses, cleans, and recompresses."""
        if compression is not None and not inplace_for_compressed:
            raise ValueError("True in-place modification is not supported for compressed files. Please provide an output_path or set inplace_for_compressed=True.")
        
        if compression is None:
            # Memory-efficient true in-place compaction for uncompressed files using os.pread/os.pwrite
            # This avoids temporary files and only requires a single pass over the data
            fd = os.open(str(vcf_path), os.O_RDWR)
            try:
                file_size: int = os.fstat(fd).st_size
                read_offset: int = 0
                write_offset: int = 0
                remainder: bytes = b""
                header_done: bool = False

                def process_data_line_bytes(line: bytes) -> bytes:
                    if use_keyword_processor:
                        # Decode → apply kp → encode. Keeps behavior consistent with string path
                        text = line.decode(encoding, errors="strict")
                        text = kp.replace_keywords(text)
                        # Collapse remaining repeated semicolons and fix ';:' → ';'
                        text = re.sub(r";{2,}", ";", text)
                        text = text.replace(";:", ";")
                        return text.encode(encoding)
                    # Fast byte-level replacements for a tiny fixed set of patterns
                    line = line.replace(b";;:", b";")
                    line = line.replace(b";:", b";")
                    line = line.replace(b";;", b";")
                    # Collapse any remaining runs of semicolons to a single ';'
                    line = re.sub(br";{2,}", b";", line)
                    return line

                while read_offset < file_size:
                    chunk: bytes = os.pread(fd, chunk_size, read_offset)
                    if not chunk:
                        break
                    read_offset += len(chunk)

                    buffer: bytes = remainder + chunk
                    # Split into lines; keep last partial line in remainder
                    parts = buffer.split(b"\n")
                    lines = parts[:-1]
                    remainder = parts[-1]

                    for line in lines:
                        # Add back newline removed by split
                        line_with_nl = line + b"\n"
                        if not header_done:
                            if line.startswith(b"#"):
                                # Header lines are written unchanged
                                os.pwrite(fd, line_with_nl, write_offset)
                                write_offset += len(line_with_nl)
                                continue
                            else:
                                header_done = True
                        # Process data lines only
                        processed = process_data_line_bytes(line_with_nl)
                        os.pwrite(fd, processed, write_offset)
                        write_offset += len(processed)

                # Handle any remaining bytes (last line without trailing newline)
                if remainder:
                    if not header_done and remainder.startswith(b"#"):
                        os.pwrite(fd, remainder, write_offset)
                        write_offset += len(remainder)
                    else:
                        processed_tail = process_data_line_bytes(remainder)
                        os.pwrite(fd, processed_tail, write_offset)
                        write_offset += len(processed_tail)

                # Ensure file ends with a newline for parsers that expect line termination
                if write_offset > 0:
                    last_byte = os.pread(fd, 1, write_offset - 1)
                    if last_byte != b"\n":
                        os.pwrite(fd, b"\n", write_offset)
                        write_offset += 1
                # Truncate file to the new, potentially smaller, size
                os.ftruncate(fd, write_offset)
            finally:
                os.close(fd)
        else:
            # In-place editing for compressed files: decompress -> clean -> recompress
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.vcf') as temp_file:
                temp_path = Path(temp_file.name)
                
                try:
                    # Decompress to temporary file
                    with gzip.open(vcf_path, 'rt', encoding='utf-8') as gzipped:
                        content = gzipped.read()
                        processed_content = process_vcf_content(content)
                        temp_file.write(processed_content)
                        temp_file.flush()
                    
                    # Recompress back to original file
                    with temp_path.open('r', encoding='utf-8') as decompressed:
                        with gzip.open(vcf_path, 'wt', encoding='utf-8') as recompressed:
                            # Process in chunks to handle large files
                            while True:
                                chunk = decompressed.read(chunk_size)
                                if not chunk:
                                    break
                                recompressed.write(chunk)
                finally:
                    # Clean up temporary file
                    if temp_path.exists():
                        temp_path.unlink()
        
        return vcf_path

    def fix_to_output() -> Path:
        """Write processed output to a new file, chunked. Handles both regular and gzip files."""
        input_is_gzipped = compression in ("gz", "bgz")
        output_should_be_gzipped = output_path.name.lower().endswith('.gz')
        
        # Determine appropriate file openers
        if input_is_gzipped:
            infile_opener = lambda: gzip.open(vcf_path, 'rt', encoding='utf-8')
        else:
            infile_opener = lambda: vcf_path.open('r', encoding='utf-8')
            
        if output_should_be_gzipped:
            outfile_opener = lambda: gzip.open(output_path, 'wt', encoding='utf-8')
        else:
            outfile_opener = lambda: output_path.open('w', encoding='utf-8')
        
        with infile_opener() as infile, outfile_opener() as outfile:
            header_done: bool = False
            while True:
                chunk: str = infile.read(chunk_size)
                if not chunk:
                    break
                if not header_done:
                    outfile.write(process_mixed_chunk(chunk))
                    header_done = '#' not in chunk  # once headers are gone
                else:
                    outfile.write(kp.replace_keywords(chunk))
        return output_path
    # -----------------------------------------------------------

    # Choose processing mode
    if output_path is None:
        return fix_true_inplace()
    else:
        output_path = Path(output_path)
        return fix_to_output()






