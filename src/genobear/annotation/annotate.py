"""
VCF annotation functions using biobear and unified database structure.
"""

import biobear as bb
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from eliot import start_action
from concurrent.futures import ThreadPoolExecutor

from genobear.config import DEFAULT_OUTPUT_FOLDER, DEFAULT_PARQUET_BATCH_SIZE
from genobear.utils.discovery import discover_annotation_databases


def extract_pathogenicity_stats(df) -> Dict[str, int]:
    """
    Extract pathogenicity statistics from annotated data.
    
    Args:
        df: DataFrame with annotation data (polars DataFrame)
        
    Returns:
        Dictionary with counts of variants by clinical significance classification
    """
    # Initialize counters for all pathogenicity categories
    stats = {
        "pathogenic": 0,
        "likely_pathogenic": 0,
        "uncertain_significance": 0,
        "likely_benign": 0,
        "benign": 0,
        "drug_response": 0,
        "risk_factor": 0,
        "protective": 0,
        "association": 0,
        "affects": 0,
        "other": 0,
        "not_provided": 0,
        "conflicting": 0
    }
    
    # Try to import polars for DataFrame operations
    try:
        import polars as pl
        
        # Check if we have INFO column (or any column containing clinical significance)
        available_columns = df.columns
        info_column = None
        
        # Look for columns that might contain clinical significance
        for col in available_columns:
            if any(term in col.lower() for term in ["info", "clnsig", "clinical", "significance", "pathogenic"]):
                info_column = col
                break
        
        if info_column is None:
            return stats
        
        # Filter non-null values and extract clinical significance patterns
        info_series = df.filter(pl.col(info_column).is_not_null())[info_column]
        
        for info_value in info_series:
            if info_value is None:
                continue
                
            info_str = str(info_value).lower()
            
            # Parse clinical significance from INFO field or direct column
            # Handle ClinVar CLNSIG format and text descriptions
            if any(term in info_str for term in ["clnsig=5", "pathogenic"]) and "likely" not in info_str:
                stats["pathogenic"] += 1
            elif any(term in info_str for term in ["clnsig=4", "likely_pathogenic", "likely pathogenic"]):
                stats["likely_pathogenic"] += 1
            elif any(term in info_str for term in ["clnsig=3", "likely_benign", "likely benign"]):
                stats["likely_benign"] += 1
            elif any(term in info_str for term in ["clnsig=2", "benign"]) and "likely" not in info_str:
                stats["benign"] += 1
            elif any(term in info_str for term in ["clnsig=0", "clnsig=1", "uncertain", "vus", "variant of uncertain significance"]):
                stats["uncertain_significance"] += 1
            elif any(term in info_str for term in ["clnsig=6", "drug", "response"]):
                stats["drug_response"] += 1
            elif any(term in info_str for term in ["risk", "factor"]):
                stats["risk_factor"] += 1
            elif any(term in info_str for term in ["protective"]):
                stats["protective"] += 1
            elif any(term in info_str for term in ["association"]):
                stats["association"] += 1
            elif any(term in info_str for term in ["affects"]):
                stats["affects"] += 1
            elif any(term in info_str for term in ["conflict", "conflicting"]):
                stats["conflicting"] += 1
            elif any(term in info_str for term in ["not_provided", "not provided"]):
                stats["not_provided"] += 1
            else:
                stats["other"] += 1
                
    except ImportError:
        # Fallback if polars is not available
        pass
    
    return stats


def report_pathogenicity_summary(stats: Dict[str, int], action) -> None:
    """
    Generate a comprehensive pathogenicity report with user-friendly summary.
    
    Args:
        stats: Pathogenicity statistics dictionary
        action: Eliot action for logging
    """
    total_variants = sum(stats.values())
    
    if total_variants == 0:
        action.log("WARNING: No pathogenicity information found in annotations!")
        return
    
    # Calculate key metrics
    pathogenic_count = stats["pathogenic"] + stats["likely_pathogenic"]
    benign_count = stats["benign"] + stats["likely_benign"]
    vus_count = stats["uncertain_significance"]
    clinical_count = pathogenic_count + benign_count + vus_count
    
    # Calculate percentages
    pathogenic_pct = (pathogenic_count / total_variants) * 100 if total_variants > 0 else 0
    benign_pct = (benign_count / total_variants) * 100 if total_variants > 0 else 0
    vus_pct = (vus_count / total_variants) * 100 if total_variants > 0 else 0
    clinical_pct = (clinical_count / total_variants) * 100 if total_variants > 0 else 0
    
    # Generate user-friendly summary
    summary = {
        "total_annotated_variants": total_variants,
        "pathogenic_variants": pathogenic_count,
        "benign_variants": benign_count,
        "vus_variants": vus_count,
        "clinical_significance_coverage": f"{clinical_pct:.1f}%",
        "pathogenic_percentage": f"{pathogenic_pct:.1f}%",
        "benign_percentage": f"{benign_pct:.1f}%",
        "vus_percentage": f"{vus_pct:.1f}%"
    }
    
    # Log the summary that users actually care about
    action.log("PATHOGENICITY SUMMARY", **summary)
    action.log("Detailed pathogenicity breakdown", **stats)
    
    # Add user-friendly interpretation
    if pathogenic_count > 0:
        action.log(f"🔴 Found {pathogenic_count} potentially disease-causing variants ({pathogenic_pct:.1f}%)")
    if benign_count > 0:
        action.log(f"🟢 Found {benign_count} likely harmless variants ({benign_pct:.1f}%)")
    if vus_count > 0:
        action.log(f"🟡 Found {vus_count} variants of uncertain significance ({vus_pct:.1f}%)")
    
    if clinical_pct < 10:
        action.log(f"⚠️  WARNING: Only {clinical_pct:.1f}% of variants have clinical significance - annotation may be incomplete!")


def annotate_vcf(
    vcf_path: Path,
    annotation_databases: Optional[List[Path]] = None,
    output_path: Optional[Path] = None,
    database_types: Optional[List[str]] = None,
    assembly: Optional[str] = None,
    join_columns: Optional[List[str]] = None,
    batch_size: int = DEFAULT_PARQUET_BATCH_SIZE,
    databases_folder: Optional[Path] = None
) -> Optional[Path]:
    """
    Annotate a VCF file with annotation databases.
    
    Args:
        vcf_path: Path to input VCF file
        annotation_databases: List of specific annotation parquet files to use
        output_path: Path for output annotated file (auto-generated if None)
        database_types: Types of databases to use (e.g., ['dbsnp', 'clinvar'])
        assembly: Genome assembly to filter databases by
        join_columns: Columns to join on (default: chrom, pos, ref, alt)
        batch_size: Batch size for streaming processing
        databases_folder: Path to databases folder (uses config default if None)
        
    Returns:
        Path to output file if successful, None if failed
    """
    if join_columns is None:
        join_columns = ["chrom", "pos", "ref", "alt"]
    
    with start_action(action_type="annotate_vcf",
                     vcf_path=str(vcf_path),
                     database_types=database_types,
                     assembly=assembly) as action:
        
        try:
            if not vcf_path.exists():
                action.log("VCF file does not exist", path=str(vcf_path))
                return None
            
            # Set default output path
            if output_path is None:
                output_path = vcf_path.parent / f"{vcf_path.stem}.annotated.vcf"
            
            # Discover annotation databases if not provided
            if annotation_databases is None:
                discovered = discover_annotation_databases(
                    databases_folder=databases_folder,
                    database_type=None,
                    assembly=assembly
                )
                
                # Filter by requested types
                if database_types:
                    discovered = {k: v for k, v in discovered.items() if k in database_types}
                
                # Collect all annotation files
                annotation_databases = []
                for files in discovered.values():
                    annotation_databases.extend(files)
                
                if not annotation_databases:
                    action.log("No annotation databases found")
                    return None
            
            action.log("Starting VCF annotation", 
                      databases_count=len(annotation_databases),
                      output_path=str(output_path))
            
            # Create biobear session
            session = bb.connect()
            
            # Build join condition
            join_conditions = []
            for col in join_columns:
                join_conditions.append(f"vcf.{col} = ann.{col}")
            join_clause = " AND ".join(join_conditions)
            
            # Create union of all annotation databases if multiple
            if len(annotation_databases) == 1:
                # Single database annotation
                reader = session.sql(f"""
                    SELECT 
                        vcf.*,
                        ann.*
                    FROM read_vcf('{vcf_path}') vcf
                    LEFT JOIN read_parquet('{annotation_databases[0]}') ann
                    ON {join_clause}
                """).to_arrow_record_batch_reader()
            else:
                # Multi-database annotation
                db_queries = []
                for db_path in annotation_databases:
                    if db_path.exists():
                        db_queries.append(f"SELECT * FROM read_parquet('{db_path}')")
                
                if not db_queries:
                    action.log("No valid annotation databases found")
                    return None
                
                union_query = " UNION ALL ".join(db_queries)
                
                reader = session.sql(f"""
                    SELECT 
                        vcf.*,
                        ann.*
                    FROM read_vcf('{vcf_path}') vcf
                    LEFT JOIN (
                        {union_query}
                    ) ann
                    ON {join_clause}
                """).to_arrow_record_batch_reader()
            
            # Create output directory
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Stream process and write output
            total_rows = 0
            batches_processed = 0
            
            if output_path.suffix.lower() == '.parquet':
                # Write to Parquet
                with pq.ParquetWriter(str(output_path), reader.schema) as writer:
                    for batch in reader:
                        if batch.num_rows > 0:
                            writer.write_batch(batch)
                            total_rows += batch.num_rows
                            batches_processed += 1
                            
                            if batches_processed % 10 == 0:
                                action.log(
                                    "Processing batches", 
                                    batches_processed=batches_processed,
                                    rows_processed=total_rows
                                )
            elif output_path.suffix.lower() == '.tsv':
                # Write as TSV
                # Build the full query for TSV export
                if len(annotation_databases) == 1:
                    query = f"""
                        COPY (
                            SELECT 
                                vcf.*,
                                ann.*
                            FROM read_vcf('{vcf_path}') vcf
                            LEFT JOIN read_parquet('{annotation_databases[0]}') ann
                            ON {join_clause}
                        ) TO '{output_path}' WITH (FORMAT CSV, DELIMITER '\t', HEADER TRUE)
                    """
                else:
                    union_query = " UNION ALL ".join(db_queries)
                    query = f"""
                        COPY (
                            SELECT 
                                vcf.*,
                                ann.*
                            FROM read_vcf('{vcf_path}') vcf
                            LEFT JOIN (
                                {union_query}
                            ) ann
                            ON {join_clause}
                        ) TO '{output_path}' WITH (FORMAT CSV, DELIMITER '\t', HEADER TRUE)
                    """
                
                session.sql(query)
                # Get total rows for logging
                count_query = f"""
                    SELECT COUNT(*) as row_count
                    FROM read_vcf('{vcf_path}') vcf
                """
                total_rows = session.sql(count_query).to_polars().get_column("row_count")[0]
            else:
                # Write as VCF (default)
                # Build the full query for VCF export
                if len(annotation_databases) == 1:
                    query = f"""
                        COPY (
                            SELECT 
                                vcf.*,
                                ann.*
                            FROM read_vcf('{vcf_path}') vcf
                            LEFT JOIN read_parquet('{annotation_databases[0]}') ann
                            ON {join_clause}
                        ) TO '{output_path}' STORED AS VCF
                    """
                else:
                    union_query = " UNION ALL ".join(db_queries)
                    query = f"""
                        COPY (
                            SELECT 
                                vcf.*,
                                ann.*
                            FROM read_vcf('{vcf_path}') vcf
                            LEFT JOIN (
                                {union_query}
                            ) ann
                            ON {join_clause}
                        ) TO '{output_path}' STORED AS VCF
                    """
                
                session.sql(query)
                # Get total rows for logging
                count_query = f"""
                    SELECT COUNT(*) as row_count
                    FROM read_vcf('{vcf_path}') vcf
                """
                total_rows = session.sql(count_query).to_polars().get_column("row_count")[0]
            
            action.log("Completed annotation", 
                      total_batches=batches_processed,
                      total_rows=total_rows)
            
            # ADD PATHOGENICITY ANALYSIS - This is what the user actually wants!
            if output_path.exists():
                try:
                    import polars as pl
                    
                    # Read the annotated output and analyze pathogenicity
                    if output_path.suffix.lower() == '.parquet':
                        annotated_df = pl.read_parquet(output_path)
                    else:
                        # For VCF/TSV outputs, we'll analyze what we can
                        action.log("Pathogenicity analysis not available for non-parquet outputs")
                        annotated_df = None
                    
                    if annotated_df is not None:
                        pathogenicity_stats = extract_pathogenicity_stats(annotated_df)
                        report_pathogenicity_summary(pathogenicity_stats, action)
                        
                        # Add pathogenicity statistics to success fields
                        action.add_success_fields(
                            pathogenicity_analysis=pathogenicity_stats,
                            pathogenic_variants=pathogenicity_stats["pathogenic"] + pathogenicity_stats["likely_pathogenic"],
                            benign_variants=pathogenicity_stats["benign"] + pathogenicity_stats["likely_benign"],
                            vus_variants=pathogenicity_stats["uncertain_significance"]
                        )
                    
                except Exception as e:
                    action.log("Failed to analyze pathogenicity", error=str(e))
            
            action.add_success_fields(
                input_size=vcf_path.stat().st_size,
                output_size=output_path.stat().st_size,
                rows_annotated=total_rows,
                databases_used=len(annotation_databases),
                success=True
            )
            
            return output_path
            
        except Exception as e:
            action.log("Annotation failed", error=str(e))
            return None


def annotate_vcf_batch(
    vcf_files: List[Path],
    output_folder: Optional[Path] = None,
    database_types: Optional[List[str]] = None,
    assembly: Optional[str] = None,
    max_concurrent: int = 2,
    force: bool = False,
    databases_folder: Optional[Path] = None
) -> Dict[Path, bool]:
    """
    Annotate multiple VCF files in batch mode.
    
    Args:
        vcf_files: List of VCF files to annotate
        output_folder: Output folder for annotated files (uses config default if None)
        database_types: Types of databases to use
        assembly: Genome assembly to filter databases by
        max_concurrent: Maximum concurrent annotations
        force: Whether to overwrite existing files
        databases_folder: Path to databases folder
        
    Returns:
        Dictionary mapping VCF files to success status
    """
    if output_folder is None:
        output_folder = DEFAULT_OUTPUT_FOLDER
    
    with start_action(action_type="annotate_vcf_batch",
                     vcf_count=len(vcf_files),
                     output_folder=str(output_folder)) as action:
        
        # Validate inputs
        valid_files = []
        for vcf_file in vcf_files:
            if vcf_file.exists():
                valid_files.append(vcf_file)
            else:
                action.log("Skipping non-existent file", file=str(vcf_file))
        
        if not valid_files:
            action.log("No valid VCF files found")
            return {}
        
        # Create output folder
        output_folder.mkdir(parents=True, exist_ok=True)
        
        # Discover databases once for all files
        discovered = discover_annotation_databases(
            databases_folder=databases_folder,
            database_type=None,
            assembly=assembly
        )
        
        # Filter by requested types
        if database_types:
            discovered = {k: v for k, v in discovered.items() if k in database_types}
        
        # Collect annotation files
        annotation_databases = []
        for files in discovered.values():
            annotation_databases.extend(files)
        
        if not annotation_databases:
            action.log("No annotation databases found")
            return {vcf_file: False for vcf_file in valid_files}
        
        action.log("Starting batch annotation",
                  databases_count=len(annotation_databases),
                  vcf_count=len(valid_files))
        
        # Process files with concurrency control
        def process_file(vcf_file: Path) -> Tuple[Path, bool]:
            output_file = output_folder / f"{vcf_file.stem}.annotated.vcf"
            
            if output_file.exists() and not force:
                action.log("Skipping existing file", output_file=str(output_file))
                return vcf_file, True
            
            result = annotate_vcf(
                vcf_file,
                annotation_databases=annotation_databases,
                output_path=output_file,
                databases_folder=databases_folder
            )
            
            return vcf_file, result is not None
        
        results = {}
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(process_file, vcf_file) for vcf_file in valid_files]
            
            for future in futures:
                try:
                    vcf_file, success = future.result()
                    results[vcf_file] = success
                except Exception as e:
                    action.log("Processing error", error=str(e))
                    # Add failed file to results - we need to know which one failed
                    # This is a bit tricky since we don't know which file failed
                    # Let's just mark all remaining as failed for now
                    for vcf_file in valid_files:
                        if vcf_file not in results:
                            results[vcf_file] = False
        
        success_count = sum(results.values())
        failed_count = len(results) - success_count
        
        action.add_success_fields(
            total_files=len(valid_files),
            successful=success_count,
            failed=failed_count
        )
        
        return results