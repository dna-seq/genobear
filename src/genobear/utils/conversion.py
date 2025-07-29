"""
File conversion utilities for GenoBear.
"""

import biobear as bb
import pyarrow as pa
import pyarrow.parquet as pq
import gzip
import tempfile
from pathlib import Path
from typing import Optional
from eliot import start_action
from biobear import VCFReadOptions, FileCompressionType


def convert_to_parquet(
    vcf_path: Path,
    output_path: Path,
    explode_infos: bool = True,
    batch_size: int = 100_000
) -> Optional[Path]:
    """
    Convert VCF file to Parquet format using biobear with streaming processing.
    
    Args:
        vcf_path: Path to input VCF file
        output_path: Path for output Parquet file
        explode_infos: Whether to explode INFO fields into separate columns
        batch_size: Number of rows to process in each batch
        
    Returns:
        Path to created Parquet file if successful, None if failed
    """
    with start_action(action_type="convert_vcf_to_parquet", 
                     input_file=str(vcf_path), 
                     output_file=str(output_path),
                     explode_infos=explode_infos,
                     batch_size=batch_size) as action:
        
        action.log("Starting streaming VCF to Parquet conversion")
        
        # Create biobear session
        session = bb.connect()
        
        # Read VCF file as streaming Arrow record batch reader
        action.log("Setting up streaming VCF reader with biobear")
        
        # Set up VCF reading options with proper compression detection
        vcf_options = VCFReadOptions(
            parse_info=explode_infos,
            parse_formats=False,
            file_compression_type=FileCompressionType.GZIP if str(vcf_path).endswith('.gz') else None
        )
        
        # Handle compression: biobear expects BGZF for .gz files, but many VCF files use regular gzip
        temp_file = None
        actual_vcf_path = vcf_path
        
        if str(vcf_path).endswith('.gz'):
            # Check if it's a regular gzip file (not BGZF) by trying to read with biobear first
            try:
                # Try with GZIP compression first (for BGZF files)
                vcf_options_test = VCFReadOptions(file_compression_type=FileCompressionType.GZIP)
                session.read_vcf_file(str(vcf_path), options=vcf_options_test).to_arrow_record_batch_reader()
                # If it works, use GZIP compression
                vcf_options = VCFReadOptions(
                    parse_info=explode_infos,
                    parse_formats=False,
                    file_compression_type=FileCompressionType.GZIP
                )
            except Exception as bgzf_error:
                action.log(f"BGZF reading failed ({bgzf_error}), trying regular gzip decompression")
                # If BGZF fails, decompress regular gzip to temporary file
                temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False)
                with gzip.open(vcf_path, 'rt') as gz_file:
                    temp_file.write(gz_file.read())
                temp_file.close()
                actual_vcf_path = Path(temp_file.name)
                action.log(f"Decompressed to temporary file: {actual_vcf_path}")
                
                vcf_options = VCFReadOptions(
                    parse_info=explode_infos,
                    parse_formats=False
                )
        else:
            # Uncompressed VCF file
            vcf_options = VCFReadOptions(
                parse_info=explode_infos,
                parse_formats=False
            )
        
        # Read VCF file using the correct biobear API
        reader = session.read_vcf_file(
            str(actual_vcf_path), 
            options=vcf_options
        ).to_arrow_record_batch_reader()
        
        # Create output directory
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Stream process batches and write to Parquet
        action.log("Starting streaming conversion to Parquet")
        
        # Get schema and filter out problematic columns (empty structs)
        original_schema = reader.schema
        action.log(f"Original schema has {len(original_schema)} fields")
        
        # Identify valid columns (exclude empty struct types)
        valid_fields = []
        invalid_columns = []
        
        for field in original_schema:
            field_type = field.type
            is_valid = True
            
            # Check for empty struct types
            if pa.types.is_struct(field_type) and len(field_type) == 0:
                is_valid = False
                invalid_columns.append(field.name)
            # Check for list of empty structs
            elif pa.types.is_list(field_type):
                value_type = field_type.value_type
                if pa.types.is_struct(value_type) and len(value_type) == 0:
                    is_valid = False
                    invalid_columns.append(field.name)
            
            if is_valid:
                valid_fields.append(field)
        
        if invalid_columns:
            action.log(f"Filtering out {len(invalid_columns)} columns with empty struct types", 
                      invalid_columns=invalid_columns)
        
        # Create filtered schema
        filtered_schema = pa.schema(valid_fields)
        valid_column_names = [field.name for field in valid_fields]
        
        # Get schema from first batch to set up Parquet writer
        total_rows = 0
        batches_processed = 0
        
        # Process the stream in batches with filtered columns
        with pq.ParquetWriter(str(output_path), filtered_schema) as writer:
            for batch in reader:
                if batch.num_rows > 0:
                    # Filter batch to only include valid columns
                    if invalid_columns:
                        filtered_batch = batch.select(valid_column_names)
                    else:
                        filtered_batch = batch
                    
                    writer.write_batch(filtered_batch)
                    total_rows += batch.num_rows
                    batches_processed += 1
                    
                    if batches_processed % 10 == 0:  # Log every 10 batches
                        action.log(
                            "Processing batches", 
                            batches_processed=batches_processed,
                            rows_processed=total_rows
                        )
        
        action.log("Completed streaming conversion", 
                  total_batches=batches_processed,
                  total_rows=total_rows)
        
        action.add_success_fields(
            input_size=vcf_path.stat().st_size,
            output_size=output_path.stat().st_size,
            rows_converted=total_rows,
            batches_processed=batches_processed,
            success=True
        )
        
        # Clean up temporary file if it was created
        if temp_file is not None:
            try:
                actual_vcf_path.unlink()
                action.log("Cleaned up temporary VCF file", temp_file_path=str(actual_vcf_path))
            except Exception as cleanup_error:
                action.log("Failed to clean up temporary file", 
                          temp_file_path=str(actual_vcf_path), 
                          error=str(cleanup_error))
        
        return output_path