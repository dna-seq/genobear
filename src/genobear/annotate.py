#!/usr/bin/env python

import typer
from typing import List, Optional, Dict, Union, Tuple
from pathlib import Path
import asyncio
from eliot import start_action
import sys
import os
import biobear as bb
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
import glob

app = typer.Typer(help="Annotate VCF files with genomic databases", no_args_is_help=True)

# Environment-configurable defaults
DEFAULT_DATABASES_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_DATABASES_FOLDER", "~/genobear/databases")))
DEFAULT_OUTPUT_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_OUTPUT_FOLDER", "~/genobear/annotations")))
DEFAULT_PARQUET_BATCH_SIZE = int(os.getenv("GENOBEAR_PARQUET_BATCH_SIZE", "100000"))
DEFAULT_MAX_CONCURRENT = int(os.getenv("GENOBEAR_MAX_CONCURRENT_ANNOTATIONS", "2"))

# Supported database types
SUPPORTED_DATABASES = {
    "dbsnp": "dbSNP - Single Nucleotide Polymorphism Database",
    "clinvar": "ClinVar - Clinical Variation Database", 
    "dbnsfp": "dbNSFP - Database for Non-synonymous SNPs",
    "hgmd": "HGMD - Human Gene Mutation Database",
    "annovar": "ANNOVAR - Functional annotation databases"
}

def discover_annotation_databases(
    databases_folder: Path,
    database_type: Optional[str] = None,
    assembly: Optional[str] = None
) -> Dict[str, List[Path]]:
    """
    Discover available annotation databases in the databases folder.
    
    Args:
        databases_folder: Path to the databases folder
        database_type: Specific database type to look for (optional)
        assembly: Specific assembly to filter by (optional)
        
    Returns:
        Dictionary mapping database types to lists of available files
    """
    with start_action(action_type="discover_databases", 
                     databases_folder=str(databases_folder),
                     database_type=database_type,
                     assembly=assembly) as action:
        
        discovered = {}
        
        if not databases_folder.exists():
            action.log("Databases folder does not exist", path=str(databases_folder))
            return discovered
        
        # Search patterns for different database types
        search_patterns = {
            "dbsnp": ["dbsnp/*/*.parquet", "dbsnp/*/*/*.parquet"],
            "clinvar": ["clinvar/*/*.parquet", "clinvar/*/*/*.parquet"],
            "dbnsfp": ["dbnsfp/*/*.parquet", "dbnsfp/*/*/*.parquet"], 
            "hgmd": ["hgmd/*/*.parquet", "hgmd/*/*/*.parquet"],
            "annovar": ["annovar/*/*.parquet", "annovar/*/*/*.parquet"]
        }
        
        databases_to_search = [database_type] if database_type else search_patterns.keys()
        
        for db_type in databases_to_search:
            if db_type not in search_patterns:
                continue
                
            discovered[db_type] = []
            
            for pattern in search_patterns[db_type]:
                search_path = databases_folder / pattern
                matches = glob.glob(str(search_path))
                
                for match in matches:
                    path = Path(match)
                    # Filter by assembly if specified
                    if assembly and assembly not in str(path):
                        continue
                    discovered[db_type].append(path)
        
        # Remove empty entries
        discovered = {k: v for k, v in discovered.items() if v}
        
        action.add_success_fields(
            discovered_databases=list(discovered.keys()),
            total_files=sum(len(files) for files in discovered.values())
        )
        
        return discovered


def annotate_vcf_with_database(
    vcf_path: Path,
    annotation_parquet: Path,
    output_path: Path,
    join_columns: List[str] = None,
    batch_size: int = DEFAULT_PARQUET_BATCH_SIZE
) -> Optional[Path]:
    """
    Annotate a VCF file with a parquet annotation database using streaming processing.
    
    Args:
        vcf_path: Path to input VCF file
        annotation_parquet: Path to annotation parquet file
        output_path: Path for output annotated file
        join_columns: Columns to join on (default: chrom, pos, ref, alt)
        batch_size: Batch size for streaming processing
        
    Returns:
        Path to output file if successful, None if failed
    """
    if join_columns is None:
        join_columns = ["chrom", "pos", "ref", "alt"]
    
    with start_action(action_type="annotate_vcf_with_database",
                     vcf_path=str(vcf_path),
                     annotation_parquet=str(annotation_parquet),
                     output_path=str(output_path)) as action:
        
        try:
            import pyarrow as pa
            
            action.log("Starting streaming VCF annotation")
            
            if not vcf_path.exists():
                action.log("VCF file does not exist", path=str(vcf_path))
                return None
                
            if not annotation_parquet.exists():
                action.log("Annotation file does not exist", path=str(annotation_parquet))
                return None
            
            # Create biobear session
            session = bb.connect()
            
            # Build join condition
            join_conditions = []
            for col in join_columns:
                join_conditions.append(f"vcf.{col} = ann.{col}")
            join_clause = " AND ".join(join_conditions)
            
            # Create streaming reader for annotation
            action.log("Setting up streaming annotation query")
            reader = session.sql(f"""
                SELECT 
                    vcf.*,
                    ann.*
                FROM read_vcf('{vcf_path}') vcf
                LEFT JOIN read_parquet('{annotation_parquet}') ann
                ON {join_clause}
            """).to_arrow_record_batch_reader()
            
            # Create output directory
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Stream process and write output
            action.log("Starting streaming annotation processing")
            
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
            else:
                # Write to VCF format using session.sql
                action.log("Converting annotated data back to VCF format")
                # This would require more complex handling to maintain VCF format
                # For now, we'll write as TSV
                output_tsv = output_path.with_suffix('.tsv')
                session.sql(f"""
                    COPY (
                        SELECT 
                            vcf.*,
                            ann.*
                        FROM read_vcf('{vcf_path}') vcf
                        LEFT JOIN read_parquet('{annotation_parquet}') ann
                        ON {join_clause}
                    ) TO '{output_tsv}' WITH (FORMAT CSV, DELIMITER '\t', HEADER TRUE)
                """)
                output_path = output_tsv
            
            action.log("Completed annotation", 
                      total_batches=batches_processed,
                      total_rows=total_rows)
            
            action.add_success_fields(
                input_size=vcf_path.stat().st_size,
                output_size=output_path.stat().st_size,
                rows_annotated=total_rows,
                batches_processed=batches_processed,
                success=True
            )
            
            return output_path
            
        except Exception as e:
            action.log("Annotation failed", error=str(e))
            return None


def annotate_vcf_multi_database(
    vcf_path: Path,
    annotation_databases: List[Path],
    output_path: Path,
    join_columns: List[str] = None,
    batch_size: int = DEFAULT_PARQUET_BATCH_SIZE
) -> Optional[Path]:
    """
    Annotate a VCF file with multiple annotation databases.
    
    Args:
        vcf_path: Path to input VCF file
        annotation_databases: List of annotation parquet files
        output_path: Path for output annotated file
        join_columns: Columns to join on
        batch_size: Batch size for streaming processing
        
    Returns:
        Path to output file if successful, None if failed
    """
    if join_columns is None:
        join_columns = ["chrom", "pos", "ref", "alt"]
    
    with start_action(action_type="annotate_vcf_multi_database",
                     vcf_path=str(vcf_path),
                     databases_count=len(annotation_databases),
                     output_path=str(output_path)) as action:
        
        try:
            if not vcf_path.exists():
                action.log("VCF file does not exist", path=str(vcf_path))
                return None
            
            # Create biobear session
            session = bb.connect()
            
            # Build multi-database join query
            join_conditions = []
            for col in join_columns:
                join_conditions.append(f"vcf.{col} = ann.{col}")
            join_clause = " AND ".join(join_conditions)
            
            # Create union of all annotation databases
            db_queries = []
            for i, db_path in enumerate(annotation_databases):
                if not db_path.exists():
                    action.log("Skipping non-existent database", path=str(db_path))
                    continue
                db_queries.append(f"SELECT * FROM read_parquet('{db_path}')")
            
            if not db_queries:
                action.log("No valid annotation databases found")
                return None
            
            union_query = " UNION ALL ".join(db_queries)
            
            action.log("Setting up multi-database annotation query")
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
            action.log("Starting multi-database annotation processing")
            
            total_rows = 0
            batches_processed = 0
            
            if output_path.suffix.lower() == '.parquet':
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
            else:
                # Write as TSV
                output_tsv = output_path.with_suffix('.tsv')
                session.sql(f"""
                    COPY (
                        SELECT 
                            vcf.*,
                            ann.*
                        FROM read_vcf('{vcf_path}') vcf
                        LEFT JOIN (
                            {union_query}
                        ) ann
                        ON {join_clause}
                    ) TO '{output_tsv}' WITH (FORMAT CSV, DELIMITER '\t', HEADER TRUE)
                """)
                output_path = output_tsv
            
            action.log("Completed multi-database annotation", 
                      total_batches=batches_processed,
                      total_rows=total_rows)
            
            action.add_success_fields(
                databases_used=len([db for db in annotation_databases if db.exists()]),
                rows_annotated=total_rows,
                success=True
            )
            
            return output_path
            
        except Exception as e:
            action.log("Multi-database annotation failed", error=str(e))
            return None


@app.command("list")
def list_databases(
    databases_folder: Path = typer.Option(
        DEFAULT_DATABASES_FOLDER,
        "--databases-folder", "-d",
        help="Path to databases folder"
    ),
    database_type: Optional[str] = typer.Option(
        None,
        "--type", "-t",
        help="Filter by database type (dbsnp, clinvar, dbnsfp, hgmd, annovar)"
    ),
    assembly: Optional[str] = typer.Option(
        None,
        "--assembly", "-a",
        help="Filter by genome assembly (hg19, hg38)"
    )
) -> None:
    """
    List available annotation databases.
    
    Example:
        python -m genobear annotate list
        python -m genobear annotate list --type dbsnp --assembly hg38
    """
    with start_action(action_type="list_annotation_databases") as action:
        typer.echo("🔍 Discovering annotation databases...")
        
        discovered = discover_annotation_databases(
            databases_folder, database_type, assembly
        )
        
        if not discovered:
            typer.echo("❌ No annotation databases found.")
            typer.echo(f"   Check that databases exist in: {databases_folder}")
            typer.echo("   Use 'python -m genobear download' to download databases first.")
            return
        
        typer.echo(f"📁 Found databases in: {databases_folder}")
        typer.echo()
        
        total_files = 0
        for db_type, files in discovered.items():
            typer.echo(f"🧬 {db_type.upper()}: {SUPPORTED_DATABASES.get(db_type, 'Unknown database')}")
            for file_path in sorted(files):
                rel_path = file_path.relative_to(databases_folder)
                file_size = file_path.stat().st_size / (1024 * 1024)  # MB
                typer.echo(f"   • {rel_path} ({file_size:.1f} MB)")
                total_files += 1
            typer.echo()
        
        typer.echo(f"📊 Total: {total_files} annotation files across {len(discovered)} database types")
        
        action.add_success_fields(
            total_databases=len(discovered),
            total_files=total_files
        )


@app.command("single")
def annotate_single(
    vcf_file: Path = typer.Argument(..., help="Input VCF file to annotate"),
    annotation_file: Path = typer.Argument(..., help="Annotation database file (parquet)"),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output file path (default: input_file.annotated.parquet)"
    ),
    join_columns: Optional[str] = typer.Option(
        "chrom,pos,ref,alt",
        "--join-on",
        help="Comma-separated columns to join on"
    ),
    batch_size: int = typer.Option(
        DEFAULT_PARQUET_BATCH_SIZE,
        "--batch-size",
        help=f"Batch size for streaming processing. Default: {DEFAULT_PARQUET_BATCH_SIZE:,}"
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Overwrite output file if it exists"
    )
) -> None:
    """
    Annotate a VCF file with a single annotation database.
    
    Example:
        python -m genobear annotate single input.vcf.gz dbsnp.parquet
        python -m genobear annotate single input.vcf.gz dbsnp.parquet --output annotated.parquet
    """
    with start_action(action_type="annotate_single_database",
                     vcf_file=str(vcf_file),
                     annotation_file=str(annotation_file)) as action:
        
        # Validate inputs
        if not vcf_file.exists():
            typer.echo(f"❌ VCF file not found: {vcf_file}")
            raise typer.Exit(1)
            
        if not annotation_file.exists():
            typer.echo(f"❌ Annotation file not found: {annotation_file}")
            raise typer.Exit(1)
        
        # Set default output path
        if output_file is None:
            output_file = vcf_file.parent / f"{vcf_file.stem}.annotated.parquet"
        
        # Check if output exists
        if output_file.exists() and not force:
            typer.echo(f"❌ Output file already exists: {output_file}")
            typer.echo("   Use --force to overwrite")
            raise typer.Exit(1)
        
        # Parse join columns
        join_cols = [col.strip() for col in join_columns.split(",")]
        
        typer.echo(f"🧬 Annotating VCF: {vcf_file.name}")
        typer.echo(f"📊 Using database: {annotation_file.name}")
        typer.echo(f"🔗 Join columns: {', '.join(join_cols)}")
        typer.echo(f"📁 Output: {output_file}")
        
        # Perform annotation
        result = annotate_vcf_with_database(
            vcf_file,
            annotation_file, 
            output_file,
            join_cols,
            batch_size
        )
        
        if result:
            output_size = result.stat().st_size / (1024 * 1024)  # MB
            typer.echo(f"✅ Annotation completed: {result.name} ({output_size:.1f} MB)")
            action.add_success_fields(success=True, output_size_mb=output_size)
        else:
            typer.echo("❌ Annotation failed")
            action.add_success_fields(success=False)
            raise typer.Exit(1)


@app.command("multi")
def annotate_multi(
    vcf_file: Path = typer.Argument(..., help="Input VCF file to annotate"),
    databases_folder: Path = typer.Option(
        DEFAULT_DATABASES_FOLDER,
        "--databases-folder", "-d",
        help="Path to databases folder"
    ),
    database_types: Optional[str] = typer.Option(
        None,
        "--types", "-t",
        help="Comma-separated database types to use (dbsnp,clinvar,dbnsfp,hgmd,annovar)"
    ),
    assembly: Optional[str] = typer.Option(
        None,
        "--assembly", "-a",
        help="Genome assembly (hg19, hg38)"
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output file path (default: input_file.annotated.parquet)"
    ),
    join_columns: Optional[str] = typer.Option(
        "chrom,pos,ref,alt",
        "--join-on",
        help="Comma-separated columns to join on"
    ),
    batch_size: int = typer.Option(
        DEFAULT_PARQUET_BATCH_SIZE,
        "--batch-size",
        help=f"Batch size for streaming processing. Default: {DEFAULT_PARQUET_BATCH_SIZE:,}"
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Overwrite output file if it exists"
    )
) -> None:
    """
    Annotate a VCF file with multiple annotation databases.
    
    Example:
        python -m genobear annotate multi input.vcf.gz --assembly hg38
        python -m genobear annotate multi input.vcf.gz --types dbsnp,clinvar --assembly hg38
    """
    with start_action(action_type="annotate_multi_database",
                     vcf_file=str(vcf_file),
                     databases_folder=str(databases_folder)) as action:
        
        # Validate inputs
        if not vcf_file.exists():
            typer.echo(f"❌ VCF file not found: {vcf_file}")
            raise typer.Exit(1)
        
        # Set default output path
        if output_file is None:
            output_file = vcf_file.parent / f"{vcf_file.stem}.multi_annotated.parquet"
        
        # Check if output exists
        if output_file.exists() and not force:
            typer.echo(f"❌ Output file already exists: {output_file}")
            typer.echo("   Use --force to overwrite")
            raise typer.Exit(1)
        
        # Parse database types
        if database_types:
            db_types = [t.strip() for t in database_types.split(",")]
        else:
            db_types = None
        
        # Discover databases
        typer.echo("🔍 Discovering annotation databases...")
        discovered = discover_annotation_databases(databases_folder, assembly=assembly)
        
        if not discovered:
            typer.echo("❌ No annotation databases found.")
            typer.echo(f"   Check that databases exist in: {databases_folder}")
            raise typer.Exit(1)
        
        # Filter by requested types
        if db_types:
            discovered = {k: v for k, v in discovered.items() if k in db_types}
            if not discovered:
                typer.echo(f"❌ No databases found for types: {', '.join(db_types)}")
                raise typer.Exit(1)
        
        # Collect all annotation files
        annotation_files = []
        for db_type, files in discovered.items():
            annotation_files.extend(files)
            typer.echo(f"📊 Using {len(files)} files from {db_type}")
        
        # Parse join columns
        join_cols = [col.strip() for col in join_columns.split(",")]
        
        typer.echo(f"🧬 Annotating VCF: {vcf_file.name}")
        typer.echo(f"🔗 Join columns: {', '.join(join_cols)}")
        typer.echo(f"📁 Output: {output_file}")
        typer.echo(f"📊 Total annotation files: {len(annotation_files)}")
        
        # Perform multi-database annotation
        result = annotate_vcf_multi_database(
            vcf_file,
            annotation_files,
            output_file,
            join_cols,
            batch_size
        )
        
        if result:
            output_size = result.stat().st_size / (1024 * 1024)  # MB
            typer.echo(f"✅ Multi-database annotation completed: {result.name} ({output_size:.1f} MB)")
            action.add_success_fields(success=True, output_size_mb=output_size)
        else:
            typer.echo("❌ Multi-database annotation failed")
            action.add_success_fields(success=False)
            raise typer.Exit(1)


@app.command("batch")
def annotate_batch(
    vcf_files: List[Path] = typer.Argument(..., help="Input VCF files to annotate"),
    databases_folder: Path = typer.Option(
        DEFAULT_DATABASES_FOLDER,
        "--databases-folder", "-d",
        help="Path to databases folder"
    ),
    output_folder: Path = typer.Option(
        DEFAULT_OUTPUT_FOLDER,
        "--output-folder", "-o",
        help="Output folder for annotated files"
    ),
    database_types: Optional[str] = typer.Option(
        None,
        "--types", "-t",
        help="Comma-separated database types to use"
    ),
    assembly: Optional[str] = typer.Option(
        None,
        "--assembly", "-a",
        help="Genome assembly (hg19, hg38)"
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT,
        "--max-concurrent",
        help=f"Maximum concurrent annotations. Default: {DEFAULT_MAX_CONCURRENT}"
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Overwrite output files if they exist"
    )
) -> None:
    """
    Annotate multiple VCF files in batch mode.
    
    Example:
        python -m genobear annotate batch *.vcf.gz --assembly hg38
        python -m genobear annotate batch file1.vcf file2.vcf --types dbsnp,clinvar
    """
    with start_action(action_type="annotate_batch",
                     vcf_count=len(vcf_files),
                     output_folder=str(output_folder)) as action:
        
        # Validate inputs
        valid_files = []
        for vcf_file in vcf_files:
            if vcf_file.exists():
                valid_files.append(vcf_file)
            else:
                typer.echo(f"⚠️  Skipping non-existent file: {vcf_file}")
        
        if not valid_files:
            typer.echo("❌ No valid VCF files found")
            raise typer.Exit(1)
        
        # Create output folder
        output_folder.mkdir(parents=True, exist_ok=True)
        
        # Discover databases
        typer.echo("🔍 Discovering annotation databases...")
        discovered = discover_annotation_databases(databases_folder, assembly=assembly)
        
        if not discovered:
            typer.echo("❌ No annotation databases found.")
            raise typer.Exit(1)
        
        # Filter by requested types
        if database_types:
            db_types = [t.strip() for t in database_types.split(",")]
            discovered = {k: v for k, v in discovered.items() if k in db_types}
        
        # Collect annotation files
        annotation_files = []
        for files in discovered.values():
            annotation_files.extend(files)
        
        typer.echo(f"📊 Found {len(annotation_files)} annotation files")
        typer.echo(f"🧬 Processing {len(valid_files)} VCF files")
        
        # Process files
        def process_file(vcf_file: Path) -> Tuple[Path, bool]:
            output_file = output_folder / f"{vcf_file.stem}.annotated.parquet"
            
            if output_file.exists() and not force:
                typer.echo(f"⏭️  Skipping existing: {output_file.name}")
                return output_file, True
            
            result = annotate_vcf_multi_database(
                vcf_file,
                annotation_files,
                output_file
            )
            
            if result:
                typer.echo(f"✅ Completed: {vcf_file.name} → {result.name}")
                return result, True
            else:
                typer.echo(f"❌ Failed: {vcf_file.name}")
                return vcf_file, False
        
        # Process with concurrency control
        success_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(process_file, vcf_file) for vcf_file in valid_files]
            
            for future in futures:
                try:
                    result_path, success = future.result()
                    if success:
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    typer.echo(f"❌ Processing error: {e}")
                    failed_count += 1
        
        typer.echo(f"📊 Batch processing completed:")
        typer.echo(f"   ✅ Successful: {success_count}")
        typer.echo(f"   ❌ Failed: {failed_count}")
        
        action.add_success_fields(
            total_files=len(valid_files),
            successful=success_count,
            failed=failed_count
        )


if __name__ == "__main__":
    app()