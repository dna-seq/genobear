"""
HGMD database processing functions.
"""

import biobear as bb
from pathlib import Path
from typing import Dict, Optional
from eliot import start_action

from genobear.config import get_database_folder
from genobear.utils.conversion import convert_to_parquet


def convert_hgmd_to_formats(
    hgmd_vcf_path: Path,
    assembly: str,
    output_folder: Optional[Path] = None,
    output_basename: Optional[str] = None,
    to_parquet: bool = True,
    to_tsv: bool = True,
    batch_size: int = 100_000
) -> Dict[str, Optional[Path]]:
    """
    Convert HGMD VCF file to Parquet and/or TSV formats using biobear.
    
    Args:
        hgmd_vcf_path: Path to input HGMD VCF file
        assembly: Genome assembly (e.g., 'hg38')
        output_folder: Output folder for converted files (uses config default if None)
        output_basename: Base name for output files
        to_parquet: Whether to generate Parquet files
        to_tsv: Whether to generate TSV files
        batch_size: Batch size for streaming conversion
        
    Returns:
        Dictionary mapping output format to file paths (None if failed)
    """
    if output_folder is None:
        output_folder = get_database_folder("hgmd")
    
    with start_action(action_type="convert_hgmd_formats",
                     input_file=str(hgmd_vcf_path),
                     output_folder=str(output_folder),
                     assembly=assembly,
                     to_parquet=to_parquet,
                     to_tsv=to_tsv) as action:
        
        results = {}
        
        # Validate input file
        if not hgmd_vcf_path.exists():
            action.log("Input HGMD VCF file does not exist", reason="input_file_not_found")
            return results
        
        # Create output folder
        assembly_folder = output_folder / assembly
        assembly_folder.mkdir(parents=True, exist_ok=True)
        
        # Determine output basename
        if not output_basename:
            output_basename = hgmd_vcf_path.stem.replace(f"_{assembly}", "").replace(".vcf", "")
        
        action.log("Starting HGMD conversion", 
                  input_size=hgmd_vcf_path.stat().st_size,
                  output_basename=output_basename)
        
        # Create biobear session
        session = bb.connect()
        
        # Convert to Parquet if requested
        if to_parquet:
            parquet_path = assembly_folder / f"{output_basename}.parquet"
            if not parquet_path.exists():
                parquet_result = convert_to_parquet(hgmd_vcf_path, parquet_path, batch_size=batch_size)
                results['parquet'] = parquet_result
            else:
                action.log("Parquet file already exists", parquet_path=str(parquet_path))
                results['parquet'] = parquet_path
        
        # Convert to TSV if requested  
        if to_tsv:
            tsv_path = assembly_folder / f"{output_basename}.tsv"
            if not tsv_path.exists():
                with start_action(action_type="convert_hgmd_to_tsv") as tsv_action:
                    # Read VCF and convert to TSV using biobear streaming
                    reader = session.sql(f"""
                        SELECT * FROM read_vcf('{hgmd_vcf_path}')
                    """).to_arrow_record_batch_reader()
                    
                    # Write TSV with header
                    first_batch = True
                    rows_written = 0
                    
                    with open(tsv_path, 'w') as tsv_file:
                        for batch in reader:
                            if batch.num_rows > 0:
                                df = batch.to_pandas()
                                
                                # Write header only for first batch
                                df.to_csv(tsv_file, sep='\t', index=False, 
                                         header=first_batch, mode='a' if not first_batch else 'w')
                                first_batch = False
                                rows_written += len(df)
                    
                    tsv_action.add_success_fields(rows_written=rows_written)
                    results['tsv'] = tsv_path
            else:
                action.log("TSV file already exists", tsv_path=str(tsv_path))
                results['tsv'] = tsv_path
        
        action.add_success_fields(
            conversion_results=results,
            successful_conversions=sum(1 for r in results.values() if r is not None)
        )
            
        return results