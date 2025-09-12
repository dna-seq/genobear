import time
from pathlib import Path
from typing import Dict, Optional

import polars as pl
from eliot import start_action
from pipefunc import pipefunc, Pipeline
from genobear.config import get_default_workers


@pipefunc(
    output_name="split_variants_dict",
    renames={"parquet_path": "vcf_parquet_path"},
    mapspec="vcf_parquet_path[i] -> split_variants_dict[i]",
)
def split_variants_by_tsa(
    parquet_path: Path, 
    explode_snv_alt: bool = True,
    write_to: Optional[Path] = None
) -> Dict[str, Path]:
    """
    Split variants in a parquet file by TSA (variant type).
    
    Args:
        parquet_path: Path to parquet file containing VCF data
        explode_snv_alt: Whether to explode ALT column on "|" separator for SNV variants
        write_to: Optional directory to write split parquet files to
        
    Returns:
        Dictionary mapping TSA (variant type) to written parquet file paths
    """
    # Skip non-parquet files
    if not parquet_path.suffix == '.parquet':
        return {}
        
    # Default output directory next to the input parquet if not provided
    if write_to is None:
        write_to = parquet_path.parent / "splitted_variants"
    write_to.mkdir(parents=True, exist_ok=True)
    
    with start_action(action_type="split_variants_by_tsa", parquet_path=str(parquet_path), explode_snv_alt=explode_snv_alt, write_to=str(write_to)) as action:
        df = pl.scan_parquet(parquet_path)
        stem = parquet_path.stem
        
        # Get unique TSAs (variant types)
        tsas = df.select("tsa").unique().collect().to_series().to_list()
        action.log(message_type="info", tsas=tsas, explode_snv_alt=explode_snv_alt)
        
        result: Dict[str, Path] = {}
        start_time = time.time()
        
        for tsa in tsas:
            df_tsa = df.filter(pl.col("tsa") == tsa)
            
            # Explode ALT column for SNV variants if requested
            if tsa == "SNV" and explode_snv_alt:
                df_tsa = df_tsa.with_columns(pl.col("alt").str.split("|")).explode("alt")
            
            # Create TSA-specific subfolder
            tsa_folder = write_to / tsa
            tsa_folder.mkdir(parents=True, exist_ok=True)
            
            # Write to parquet file
            where = tsa_folder / f"{stem}.parquet"
            action.log(message_type="info", tsa=tsa, where=str(where))
            df_tsa.sink_parquet(where)
            result[tsa] = where
        
        # Calculate execution time
        end_time = time.time()
        elapsed_seconds = end_time - start_time
        minutes = int(elapsed_seconds // 60)
        seconds = elapsed_seconds % 60
        execution_time = f"{minutes}:{seconds:06.3f}"
        
        action.log(message_type="info", execution_time=execution_time, tsas=tsas, result_count=len(result))
        return result


def make_parquet_splitting_pipeline() -> Pipeline:
    """Create a pipeline that only does splitting with parquet files as input."""
    return Pipeline([split_variants_by_tsa], print_error=True)


def make_vcf_splitting_pipeline() -> Pipeline:
    """Create a pipeline that combines VCF download/conversion with splitting using pipeline composition."""
    from genobear.pipelines.vcf_downloader import make_vcf_pipeline
    
    # Compose the VCF download pipeline with the splitting pipeline
    vcf_pipeline = make_vcf_pipeline()
    splitting_pipeline = make_parquet_splitting_pipeline()
    
    # Use the | operator to compose pipelines
    return vcf_pipeline | splitting_pipeline


def split_parquet_variants(
    vcf_parquet_path: Path | list[Path],
    explode_snv_alt: bool = True,
    write_to: Optional[Path] = None,
    parallel: bool = True,
    workers: Optional[int] = None,
    return_results: bool = True,
    **kwargs
) -> dict:
    """Split variants in parquet files by TSA using the parquet splitting pipeline.
    
    Args:
        vcf_parquet_path: Path or list of paths to parquet files containing VCF data
        explode_snv_alt: Whether to explode ALT column on "|" separator for SNV variants
        write_to: Optional directory to write split parquet files to
        parallel: Run pipeline in parallel (default True)
        return_results: Return results dict (default True)
        **kwargs: Additional parameters passed to pipeline.map()
    
    Returns:
        Dictionary with pipeline results containing 'split_variants_dict'
    """
    pipeline = make_parquet_splitting_pipeline()
    
    # Handle single path or list of paths
    if isinstance(vcf_parquet_path, Path):
        vcf_parquet_path = [vcf_parquet_path]
    
    inputs = {
        "vcf_parquet_path": vcf_parquet_path,
        "explode_snv_alt": explode_snv_alt,
        "write_to": write_to,
        **kwargs
    }
    
    # Determine workers for potential parallel execution
    if workers is None:
        workers = get_default_workers()

    with start_action(action_type="split_parquet_variants", paths=[str(p) for p in vcf_parquet_path], explode_snv_alt=explode_snv_alt):
        results = pipeline.map(
            inputs=inputs,
            output_names={"split_variants_dict"},
            parallel=parallel and (workers > 1),
            return_results=return_results,
            executor=(None if not (parallel and workers > 1) else __import__("concurrent").futures.ProcessPoolExecutor(max_workers=workers))
        )
        return results


def download_convert_and_split_vcf(
    url: str,
    pattern: str | None = None,
    name: str | Path = "downloads",
    output_names: set[str] | None = None,
    parallel: bool = True,
    return_results: bool = True,
    explode_snv_alt: bool = True,
    **kwargs
) -> dict:
    """Download, convert VCF files to parquet, and split variants by TSA using the composed pipeline.
    
    Args:
        url: Base URL to search for VCF files
        pattern: Regex pattern to filter files (optional)
        name: Directory name or Path for downloads
        output_names: Which outputs to return (defaults to parquet_path and split_variants_dict)
        parallel: Run pipeline in parallel (default True)
        return_results: Return results dict (default True)
        explode_snv_alt: Whether to explode ALT column on "|" separator for SNV variants
        **kwargs: Additional parameters passed to pipeline.map()
    
    Returns:
        Dictionary with pipeline results containing 'vcf_parquet_path' and 'split_variants_dict' by default
    """
    pipeline = make_vcf_splitting_pipeline()
    
    # Default to the most useful outputs including split variants
    if output_names is None:
        output_names = {"vcf_parquet_path", "split_variants_dict"}
    
    inputs = {
        "url": url,
        "pattern": pattern,
        "file_only": True,
        "name": name,
        "check_files": True,
        "expiry_time": 7 * 24 * 3600,  # 7 days
        "explode_snv_alt": explode_snv_alt,
        **kwargs
    }
    
    with start_action(action_type="download_convert_and_split_vcf", url=url, name=str(name), explode_snv_alt=explode_snv_alt):
        results = pipeline.map(
            inputs=inputs,
            output_names=output_names,
            parallel=parallel,
            return_results=return_results,
        )
        return results


def make_clinvar_splitting_pipeline() -> Pipeline:
    """Create a pipeline with ClinVar-specific defaults and variant splitting."""
    from genobear.pipelines.helpers import make_clinvar_pipeline as make_clinvar_helper
    return make_clinvar_helper(with_splitting=True)


def make_ensembl_vcf_splitting_pipeline() -> Pipeline:
    """Create a pipeline with Ensembl VCF-specific defaults and variant splitting."""
    from genobear.pipelines.helpers import make_ensembl_pipeline as make_ensembl_helper
    return make_ensembl_helper(with_splitting=True)


if __name__ == "__main__":
    # Example 1: Split existing parquet files
    print("=== Parquet Splitting Example ===")
    parquet_files = [Path("/home/antonkulaga/data/download/clinvar/clinvar.parquet")]
    
    if all(p.exists() for p in parquet_files):
        split_results = split_parquet_variants(
            vcf_parquet_path=parquet_files,
            explode_snv_alt=True,
            write_to=Path("/home/antonkulaga/data/download/split_test")
        )
        
        print("Parquet splitting completed!")
        print(f"Results: {list(split_results.keys())}")
        
        # Print split variant paths
        if "split_variants_dict" in split_results:
            split_result = split_results["split_variants_dict"]
            split_out = split_result.output
            
            # Extract split variant dictionaries
            if hasattr(split_out, "ravel"):
                split_seq = split_out.ravel().tolist()
            elif isinstance(split_out, list):
                split_seq = split_out
            else:
                split_seq = [split_out]
            
            print(f"\nGenerated split variants for {len(split_seq)} file(s):")
            for i, split_dict in enumerate(split_seq):
                if isinstance(split_dict, dict):
                    print(f"  File {i+1}:")
                    for tsa, path in split_dict.items():
                        print(f"    {tsa}: {path}")
                else:
                    print(f"  File {i+1}: {split_dict}")
    else:
        print("Parquet files not found, skipping parquet splitting example")
    
    print("\n" + "="*50)
    
    # Example 2: Full pipeline with download, conversion, and splitting
    print("=== Full Download + Conversion + Splitting Pipeline ===")
    full_results = download_convert_and_split_vcf(
        url="https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/",
        pattern=r"clinvar\.vcf\.gz$",
        name="clinvar_full_test",
        dest_dir=Path("/home/antonkulaga/data/download"),
        explode_snv_alt=True
    )
    
    print("Full pipeline completed!")
    print(f"Results: {list(full_results.keys())}")
    
    # Print split variant paths
    if "split_variants_dict" in full_results:
        split_result = full_results["split_variants_dict"]
        split_out = split_result.output
        
        # Extract split variant dictionaries
        if hasattr(split_out, "ravel"):
            split_seq = split_out.ravel().tolist()
        elif isinstance(split_out, list):
            split_seq = split_out
        else:
            split_seq = [split_out]
        
        print(f"\nGenerated split variants for {len(split_seq)} file(s):")
        for i, split_dict in enumerate(split_seq):
            if isinstance(split_dict, dict):
                print(f"  File {i+1}:")
                for tsa, path in split_dict.items():
                    print(f"    {tsa}: {path}")
            else:
                print(f"  File {i+1}: {split_dict}")
