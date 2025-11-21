"""Parse VCF files to extract chromosome information and variants for annotation."""

from pathlib import Path
from typing import Union

import polars as pl
from eliot import start_action
from pipefunc import pipefunc

from genobear.io import read_vcf_file


@pipefunc(output_name="vcf_chromosomes", cache=False)
def extract_chromosomes_from_vcf(vcf_lazy_frame: pl.LazyFrame) -> list[str]:
    """
    Extract unique chromosome identifiers from a VCF LazyFrame efficiently.
    
    This function extracts unique chromosomes by only collecting the chromosome
    column data, not the entire VCF.
    
    Args:
        vcf_lazy_frame: LazyFrame containing VCF data
        
    Returns:
        List of unique chromosome identifiers (e.g., ['1', '2', 'X', 'Y'])
        
    Example:
        >>> vcf_lf = load_vcf_as_lazy_frame("sample.vcf.gz")
        >>> chromosomes = extract_chromosomes_from_vcf(vcf_lf)
        >>> print(chromosomes)
        ['1', '2', '3', 'X']
    """
    with start_action(
        action_type="extract_chromosomes_from_vcf"
    ) as action:
        # Extract unique chromosome values - only materializes the chrom column
        chromosomes = (
            vcf_lazy_frame
            .select(pl.col("chrom"))
            .unique()
            .collect()
            .get_column("chrom")
            .to_list()
        )
        
        # Sort chromosomes for consistency (handle numeric and non-numeric)
        def sort_key(chrom: str) -> tuple:
            """Sort key that handles numeric and non-numeric chromosomes."""
            try:
                return (0, int(chrom))
            except ValueError:
                return (1, chrom)
        
        chromosomes = sorted(chromosomes, key=sort_key)
        
        action.log(
            message_type="info",
            step="chromosomes_extracted",
            num_chromosomes=len(chromosomes),
            chromosomes=chromosomes
        )
        
        return chromosomes


@pipefunc(output_name="vcf_lazy_frame", cache=False)
def load_vcf_as_lazy_frame(
    vcf_path: Union[str, Path],
    info_fields: Union[list[str], None] = None,
) -> pl.LazyFrame:
    """
    Load VCF file as a Polars LazyFrame for annotation.
    
    This function reads the VCF file and returns a LazyFrame that can be
    used for joining with annotation data.
    
    Args:
        vcf_path: Path to the VCF file (can be .vcf or .vcf.gz)
        info_fields: List of INFO fields to extract (if None, extracts all)
        
    Returns:
        Polars LazyFrame containing VCF data
        
    Example:
        >>> vcf_lf = load_vcf_as_lazy_frame("sample.vcf.gz")
        >>> print(vcf_lf.schema)
    """
    with start_action(
        action_type="load_vcf_as_lazy_frame",
        vcf_path=str(vcf_path)
    ) as action:
        # Read VCF file using genobear's read_vcf_file
        vcf_lazy = read_vcf_file(vcf_path, info_fields=info_fields, save_parquet=None)
        
        # Get schema info for logging
        schema = vcf_lazy.collect_schema()
        
        action.log(
            message_type="info",
            step="vcf_loaded",
            num_columns=len(schema),
            columns=list(schema.names())
        )
        
        return vcf_lazy

