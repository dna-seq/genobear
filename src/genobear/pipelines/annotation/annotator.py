"""Annotation logic for joining VCF data with ensembl_variations reference data."""

from pathlib import Path
from typing import Optional

import polars as pl
from eliot import start_action
from pipefunc import pipefunc


@pipefunc(output_name="annotation_parquet_paths", cache=False)
def find_annotation_parquet_files(
    ensembl_cache_path_checked: Path,
    vcf_chromosomes: list[str],
    variant_type: str = "SNV",
) -> dict[str, Path]:
    """
    Find annotation parquet files for specified chromosomes.
    
    Args:
        ensembl_cache_path_checked: Path to ensembl_variations cache directory
        vcf_chromosomes: List of chromosome identifiers to find
        variant_type: Variant type to use (default: "SNV")
        
    Returns:
        Dictionary mapping chromosome to parquet file path
        
    Example:
        >>> paths = find_annotation_parquet_files(cache_path, ['1', '2', 'X'])
        >>> print(paths)
        {'1': Path('...data/SNV/homo_sapiens-chr1.parquet'), ...}
    """
    with start_action(
        action_type="find_annotation_parquet_files",
        ensembl_cache_path=str(ensembl_cache_path_checked),
        num_chromosomes=len(vcf_chromosomes),
        variant_type=variant_type
    ) as action:
        parquet_map = {}
        
        # Look for files in {variant_type}/ directory structure
        # Try both data/{variant_type}/ and {variant_type}/ patterns
        variant_dir = ensembl_cache_path_checked / variant_type
        if not variant_dir.exists():
            # Fallback to data/{variant_type}/ structure
            variant_dir = ensembl_cache_path_checked / "data" / variant_type
        
        if not variant_dir.exists():
            action.log(
                message_type="warning",
                step="variant_dir_not_found",
                variant_dir=str(variant_dir),
                cache_path=str(ensembl_cache_path_checked)
            )
            return parquet_map
        
        for chrom in vcf_chromosomes:
            # Try different file naming patterns
            possible_names = [
                f"homo_sapiens-chr{chrom}.parquet",
                f"chr{chrom}.parquet",
                f"{chrom}.parquet",
            ]
            
            for name in possible_names:
                parquet_path = variant_dir / name
                if parquet_path.exists():
                    parquet_map[chrom] = parquet_path
                    break
            
            if chrom not in parquet_map:
                action.log(
                    message_type="warning",
                    step="parquet_not_found_for_chromosome",
                    chromosome=chrom,
                    variant_dir=str(variant_dir)
                )
        
        action.log(
            message_type="info",
            step="annotation_files_found",
            num_found=len(parquet_map),
            chromosomes=list(parquet_map.keys())
        )
        
        return parquet_map


@pipefunc(output_name="annotated_vcf_lazy", cache=False)
def annotate_vcf_with_ensembl(
    vcf_lazy_frame: pl.LazyFrame,
    annotation_parquet_paths: dict[str, Path],
    join_columns: Optional[list[str]] = None,
) -> pl.LazyFrame:
    """
    Annotate VCF data with ensembl_variations reference data using lazy joins.
    
    This function performs lazy joins between the VCF data and reference annotation
    data for each chromosome. The result is a single LazyFrame with all annotations.
    
    Args:
        vcf_lazy_frame: LazyFrame containing VCF data
        annotation_parquet_paths: Dictionary mapping chromosome to parquet file path
        join_columns: Columns to join on (default: ["chromosome", "start", "reference", "alternate"])
        
    Returns:
        Annotated LazyFrame with ensembl_variations data joined
        
    Example:
        >>> annotated_lf = annotate_vcf_with_ensembl(vcf_lf, parquet_paths)
        >>> result = annotated_lf.collect()
    """
    with start_action(
        action_type="annotate_vcf_with_ensembl",
        num_chromosomes=len(annotation_parquet_paths)
    ) as action:
        # Default join columns for VCF to ensembl_variations matching
        # Note: VCF uses 'chrom', 'ref', 'alt' while ensembl may use 'chromosome', 'reference', 'alternate'
        if join_columns is None:
            join_columns = ["chrom", "start", "ref", "alt"]
        
        action.log(
            message_type="info",
            step="preparing_annotation",
            join_columns=join_columns
        )
        
        # If no annotation files found, return original VCF
        if not annotation_parquet_paths:
            action.log(
                message_type="warning",
                step="no_annotation_files_returning_original"
            )
            return vcf_lazy_frame
        
        # Process each chromosome separately and collect results
        annotated_parts = []
        
        for chrom, parquet_path in annotation_parquet_paths.items():
            action.log(
                message_type="info",
                step="annotating_chromosome",
                chromosome=chrom,
                parquet_path=str(parquet_path)
            )
            
            # Filter VCF for this chromosome
            vcf_chrom = vcf_lazy_frame.filter(pl.col("chrom") == chrom)
            
            # Load annotation data for this chromosome
            annotation_lf = pl.scan_parquet(parquet_path)
            
            # Perform left join to keep all VCF variants
            # Using "left" join ensures we don't lose any variants from the input VCF
            annotated_chrom = vcf_chrom.join(
                annotation_lf,
                on=join_columns,
                how="left",
                suffix="_ensembl"
            )
            
            annotated_parts.append(annotated_chrom)
        
        # Concatenate all annotated chromosome parts
        if annotated_parts:
            result = pl.concat(annotated_parts)
            action.log(
                message_type="info",
                step="annotation_complete",
                num_chromosome_parts=len(annotated_parts)
            )
        else:
            # If no parts were created, return original VCF
            result = vcf_lazy_frame
            action.log(
                message_type="warning",
                step="no_annotation_parts_created"
            )
        
        return result


@pipefunc(output_name="annotated_vcf_path", cache=False)
def save_annotated_vcf(
    annotated_vcf_lazy: pl.LazyFrame,
    output_path: Optional[Path] = None,
    compression: str = "zstd",
) -> Path:
    """
    Save annotated VCF LazyFrame to parquet file.
    
    Args:
        annotated_vcf_lazy: Annotated LazyFrame to save
        output_path: Path where to save the parquet file (if None, returns without saving)
        compression: Compression type for parquet (default: "zstd")
        
    Returns:
        Path to the saved parquet file
        
    Example:
        >>> path = save_annotated_vcf(annotated_lf, Path("annotated.parquet"))
        >>> print(f"Saved to: {path}")
    """
    with start_action(
        action_type="save_annotated_vcf",
        output_path=str(output_path) if output_path else None,
        compression=compression
    ) as action:
        if output_path is None:
            action.log(
                message_type="info",
                step="no_output_path_skipping_save"
            )
            # Return a temp path or raise error
            raise ValueError("output_path is required for saving annotated VCF")
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Use sink_parquet for memory-efficient writing
        annotated_vcf_lazy.sink_parquet(
            output_path,
            compression=compression
        )
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        action.log(
            message_type="info",
            step="annotation_saved",
            output_path=str(output_path),
            file_size_mb=round(file_size_mb, 2)
        )
        
        return output_path

