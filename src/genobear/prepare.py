"""
GenoBear Prepare CLI - Modern pipeline-based data preparation.

This module provides a CLI interface using the Pipelines class for better
parallelization, caching, and pipeline composition.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, List

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from eliot import start_action

from dotenv import load_dotenv

logs = Path("logs") if Path("logs").exists() else Path.cwd().parent / "logs"

load_dotenv()

# Set POLARS_VERBOSE from env if not already set (default: 0 for clean output)
if "POLARS_VERBOSE" not in os.environ:
    os.environ["POLARS_VERBOSE"] = "0"

from genobear import PreparationPipelines
from pycomfort.logging import to_nice_file, to_nice_stdout

# Create the main CLI app
app = typer.Typer(
    name="genobear-prepare",
    help="Modern Genomic Data Pipeline Tool (using Pipelines class)",
    rich_markup_mode="rich",
    no_args_is_help=True
)

console = Console()


@app.command()
def ensembl(
    dest_dir: Optional[str] = typer.Option(
        None,
        "--dest-dir",
        help="Destination directory for downloads. If not specified, uses platformdirs cache."
    ),
    split: bool = typer.Option(
        False,
        "--split/--no-split",
        help="Split downloaded parquet files by variant type (TSA)"
    ),
    download_workers: Optional[int] = typer.Option(
        None,
        "--download-workers",
        help="Number of workers for parallel downloads (default: GENOBEAR_DOWNLOAD_WORKERS or CPU count)"
    ),
    parquet_workers: Optional[int] = typer.Option(
        None,
        "--parquet-workers",
        help="Number of workers for parquet operations (default: GENOBEAR_PARQUET_WORKERS or 4)"
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        help="Number of workers for general processing (default: GENOBEAR_WORKERS or CPU count)"
    ),
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Timeout in seconds for downloads. Defaults to 3600 (1 hour)"
    ),
    run_folder: Optional[str] = typer.Option(
        None,
        "--run-folder",
        help="Optional run folder for pipeline execution and caching"
    ),
    log: bool = typer.Option(
        True,
        "--log/--no-log",
        help="Enable detailed logging to files"
    ),
    pattern: Optional[str] = typer.Option(
        None,
        "--pattern",
        help="Regex pattern to filter files. Examples: 'chr(21|22)' for chr21&22, 'chr2[12]' for chr21&22, 'chr(X|Y)' for sex chromosomes. Default: all chromosomes"
    ),
    url: Optional[str] = typer.Option(
        None,
        "--url",
        help="Base URL for Ensembl data (default: https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/)"
    ),
    explode_snv_alt: bool = typer.Option(
        True,
        "--explode-snv-alt/--no-explode-snv-alt",
        help="Explode ALT column for SNV variants when splitting"
    ),
    upload: bool = typer.Option(
        False,
        "--upload/--no-upload",
        help="Upload parquet files to Hugging Face Hub after processing"
    ),
    repo_id: str = typer.Option(
        "just-dna-seq/ensembl_variations",
        "--repo-id",
        help="Hugging Face repository ID for upload"
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Hugging Face API token (uses HF_TOKEN env var if not provided)"
    ),
):
    """
    Download Ensembl variation VCF files using the Pipelines approach.
    
    This uses the modern Pipelines class which provides better parallelization,
    caching, and pipeline composition features.
    
    Downloads VCF files from Ensembl FTP, converts them to parquet, and optionally
    splits them by variant type. Can also upload results directly to Hugging Face Hub.
    
    To download specific chromosomes, use --pattern:
      prepare ensembl --pattern "chr(21|22)"  # chromosomes 21 and 22
      prepare ensembl --pattern "chr(X|Y)"    # sex chromosomes
      prepare ensembl --pattern "chr[1-9]"    # chromosomes 1-9
    
    Example with upload:
      prepare ensembl --split --upload
      prepare ensembl --upload --repo-id username/my-dataset
    """
    # Configure logging for this command
    if log:
        logs.mkdir(exist_ok=True, parents=True)
        to_nice_file(logs / "prepare_ensembl.json", logs / "prepare_ensembl.log")
        to_nice_stdout()
    
    with start_action(action_type="prepare_ensembl_command") as action:
        action.log(
            message_type="info",
            dest_dir=dest_dir,
            pattern=pattern,
            split=split,
            workers=workers,
            download_workers=download_workers,
            parquet_workers=parquet_workers,
            timeout=timeout,
            run_folder=run_folder,
            upload=upload
        )
        
        console.print("🔧 Setting up Ensembl pipeline using Pipelines class...")
        
        # Build pipeline
        pipeline = PreparationPipelines.ensembl(with_splitting=split)
        
        # Prepare inputs
        inputs = {}
        
        if dest_dir is not None:
            inputs["dest_dir"] = Path(dest_dir)
        
        if timeout is not None:
            inputs["timeout"] = timeout
        
        if pattern is not None:
            inputs["pattern"] = pattern
            console.print(f"📋 Using pattern filter: [bold cyan]{pattern}[/bold cyan]")
        
        if url is not None:
            inputs["url"] = url
        
        if split and explode_snv_alt is not None:
            inputs["explode_snv_alt"] = explode_snv_alt
        
        # Show effective configuration
        effective_dest = dest_dir if dest_dir else "platformdirs cache"
        console.print(f"📁 Destination: [bold blue]{effective_dest}[/bold blue]")
        console.print(f"🔄 Splitting: [bold blue]{split}[/bold blue]")
        console.print(f"👷 General workers: [bold blue]{workers or 'auto'}[/bold blue]")
        console.print(f"📥 Download workers: [bold blue]{download_workers or 'auto'}[/bold blue]")
        console.print(f"🔄 Parquet workers: [bold blue]{parquet_workers or 'auto (4)'}[/bold blue]")
        
        # Execute pipeline
        console.print("🚀 Starting pipeline execution...")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            task = progress.add_task("Running pipeline...", total=None)
            
            results = PreparationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=None,  # Get all outputs
                run_folder=run_folder,
                return_results=True,
                show_progress="rich" if log else False,
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )
            
            progress.update(task, description="✅ Pipeline completed")
        
        # Report results
        console.print("\n✅ Pipeline execution completed!")
        
        if "vcf_parquet_path" in results:
            parquet_files = results["vcf_parquet_path"]
            if isinstance(parquet_files, list):
                console.print(f"📦 Converted {len(parquet_files)} parquet files")
            else:
                console.print(f"📦 Parquet file: {parquet_files}")
        
        if "split_variants_dict" in results:
            split_dict = results["split_variants_dict"]
            if isinstance(split_dict, dict):
                console.print(f"🔀 Split variants into {len(split_dict)} categories")
                for variant_type, paths in split_dict.items():
                    if isinstance(paths, list):
                        console.print(f"  - {variant_type}: {len(paths)} files")
                    else:
                        console.print(f"  - {variant_type}: {paths}")
        
        action.log(message_type="success", result_keys=list(results.keys()))
        
        # Upload to Hugging Face if requested
        if upload:
            console.print("\n🔄 Starting upload to Hugging Face...")
            console.print(f"📦 Repository: [bold cyan]{repo_id}[/bold cyan]")
            
            # Determine upload source directory
            # If splitting was enabled, upload from splitted_variants subdirectory
            upload_source_dir = None
            if dest_dir:
                upload_source_dir = Path(dest_dir)
                if split:
                    upload_source_dir = upload_source_dir / "splitted_variants"
            elif split:
                # Default cache location with splitted_variants subdirectory
                from platformdirs import user_cache_dir
                user_cache_path = Path(user_cache_dir(appname="genobear"))
                upload_source_dir = user_cache_path / "ensembl_variations" / "splitted_variants"
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True
            ) as progress:
                task = progress.add_task("Uploading files...", total=None)
                
                upload_results = PreparationPipelines.upload_ensembl_to_hf(
                    source_dir=upload_source_dir,
                    repo_id=repo_id,
                    token=token,
                    workers=workers,
                    log=log,
                )
                
                progress.update(task, description="✅ Upload completed")
            
            # Report upload results (now from batch upload)
            uploaded_files = upload_results.get("uploaded_files", [])
            num_uploaded = upload_results.get("num_uploaded", 0)
            num_skipped = upload_results.get("num_skipped", 0)
            
            console.print(f"\n📊 Upload Summary:")
            console.print(f"  - Total files: [bold]{len(uploaded_files)}[/bold]")
            console.print(f"  - Uploaded: [bold green]{num_uploaded}[/bold green]")
            console.print(f"  - Skipped (size match): [bold yellow]{num_skipped}[/bold yellow]")
            
            action.log(
                message_type="upload_summary",
                total=len(uploaded_files),
                uploaded=num_uploaded,
                skipped=num_skipped
            )


@app.command()
def clinvar(
    dest_dir: Optional[str] = typer.Option(
        None,
        "--dest-dir",
        help="Destination directory for downloads. If not specified, uses platformdirs cache."
    ),
    split: bool = typer.Option(
        False,
        "--split/--no-split",
        help="Split downloaded parquet files by variant type (TSA)"
    ),
    download_workers: Optional[int] = typer.Option(
        None,
        "--download-workers",
        help="Number of workers for parallel downloads"
    ),
    parquet_workers: Optional[int] = typer.Option(
        None,
        "--parquet-workers",
        help="Number of workers for parquet conversion (default: 4)"
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        help="Number of workers for general processing"
    ),
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Timeout in seconds for downloads"
    ),
    run_folder: Optional[str] = typer.Option(
        None,
        "--run-folder",
        help="Optional run folder for pipeline execution"
    ),
    log: bool = typer.Option(
        True,
        "--log/--no-log",
        help="Enable detailed logging to files"
    ),
    upload: bool = typer.Option(
        False,
        "--upload/--no-upload",
        help="Upload parquet files to Hugging Face Hub after processing"
    ),
    repo_id: str = typer.Option(
        "just-dna-seq/clinvar",
        "--repo-id",
        help="Hugging Face repository ID for upload"
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Hugging Face API token (uses HF_TOKEN env var if not provided)"
    ),
):
    """
    Download ClinVar VCF files using the Pipelines approach.
    
    Downloads ClinVar data from NCBI FTP, converts to parquet, and optionally
    splits by variant type. Can also upload results directly to Hugging Face Hub.
    
    Example with upload:
        prepare clinvar --split --upload
        prepare clinvar --upload --repo-id username/my-dataset
    """
    if log:
        logs.mkdir(exist_ok=True, parents=True)
        to_nice_file(logs / "prepare_clinvar.json", logs / "prepare_clinvar.log")
        to_nice_stdout()
    
    with start_action(action_type="prepare_clinvar_command") as action:
        action.log(
            message_type="info",
            dest_dir=dest_dir,
            split=split,
            workers=workers,
            download_workers=download_workers,
            upload=upload
        )
        
        console.print("🔧 Setting up ClinVar pipeline...")
        console.print("🚀 Executing pipeline...")
        
        results = PreparationPipelines.download_clinvar(
            dest_dir=Path(dest_dir) if dest_dir else None,
            with_splitting=split,
            download_workers=download_workers,
            parquet_workers=parquet_workers,
            workers=workers,
            log=log,
            timeout=timeout,
            run_folder=run_folder,
        )
        
        console.print("✅ ClinVar download completed!")
        action.log(message_type="success", result_keys=list(results.keys()))
        
        # Upload to Hugging Face if requested
        if upload:
            console.print("\n🔄 Starting upload to Hugging Face...")
            console.print(f"📦 Repository: [bold cyan]{repo_id}[/bold cyan]")
            
            # Determine upload source directory
            # If splitting was enabled, upload from splitted_variants subdirectory
            upload_source_dir = None
            if dest_dir:
                upload_source_dir = Path(dest_dir)
                if split:
                    upload_source_dir = upload_source_dir / "splitted_variants"
            elif split:
                # Default cache location with splitted_variants subdirectory
                from platformdirs import user_cache_dir
                user_cache_path = Path(user_cache_dir(appname="genobear"))
                upload_source_dir = user_cache_path / "clinvar" / "splitted_variants"
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True
            ) as progress:
                task = progress.add_task("Uploading files...", total=None)
                
                upload_results = PreparationPipelines.upload_clinvar_to_hf(
                    source_dir=upload_source_dir,
                    repo_id=repo_id,
                    token=token,
                    workers=workers,
                    log=log,
                )
                
                progress.update(task, description="✅ Upload completed")
            
            # Report upload results (now from batch upload)
            uploaded_files = upload_results.get("uploaded_files", [])
            num_uploaded = upload_results.get("num_uploaded", 0)
            num_skipped = upload_results.get("num_skipped", 0)
            
            console.print(f"\n📊 Upload Summary:")
            console.print(f"  - Total files: [bold]{len(uploaded_files)}[/bold]")
            console.print(f"  - Uploaded: [bold green]{num_uploaded}[/bold green]")
            console.print(f"  - Skipped (size match): [bold yellow]{num_skipped}[/bold yellow]")
            
            action.log(
                message_type="upload_summary",
                total=len(uploaded_files),
                uploaded=num_uploaded,
                skipped=num_skipped
            )


# @app.command()  # Temporarily disabled - not fully implemented
def dbsnp(
    dest_dir: Optional[str] = typer.Option(
        None,
        "--dest-dir",
        help="Destination directory for downloads"
    ),
    build: str = typer.Option(
        "GRCh38",
        "--build",
        help="Genome build (GRCh38 or GRCh37)"
    ),
    split: bool = typer.Option(
        False,
        "--split/--no-split",
        help="Split downloaded parquet files by variant type"
    ),
    download_workers: Optional[int] = typer.Option(
        None,
        "--download-workers",
        help="Number of workers for parallel downloads"
    ),
    parquet_workers: Optional[int] = typer.Option(
        None,
        "--parquet-workers",
        help="Number of workers for parquet conversion (default: 4)"
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        help="Number of workers for general processing"
    ),
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Timeout in seconds for downloads"
    ),
    run_folder: Optional[str] = typer.Option(
        None,
        "--run-folder",
        help="Optional run folder for pipeline execution"
    ),
    log: bool = typer.Option(
        True,
        "--log/--no-log",
        help="Enable detailed logging to files"
    ),
):
    """
    Download dbSNP VCF files using the Pipelines approach.
    
    Downloads dbSNP data from NCBI FTP for the specified genome build.
    """
    if log:
        logs.mkdir(exist_ok=True, parents=True)
        to_nice_file(logs / "prepare_dbsnp.json", logs / "prepare_dbsnp.log")
        to_nice_stdout()
    
    with start_action(action_type="prepare_dbsnp_command") as action:
        action.log(
            message_type="info",
            dest_dir=dest_dir,
            build=build,
            split=split
        )
        
        console.print(f"🔧 Setting up dbSNP pipeline for {build}...")
        
        results = PreparationPipelines.download_dbsnp(
            dest_dir=Path(dest_dir) if dest_dir else None,
            build=build,
            with_splitting=split,
            download_workers=download_workers,
            parquet_workers=parquet_workers,
            workers=workers,
            log=log,
            timeout=timeout,
            run_folder=run_folder,
        )
        
        console.print(f"✅ dbSNP {build} download completed!")
        action.log(message_type="success", result_keys=list(results.keys()))


# @app.command()  # Temporarily disabled - not fully implemented
def gnomad(
    dest_dir: Optional[str] = typer.Option(
        None,
        "--dest-dir",
        help="Destination directory for downloads"
    ),
    version: str = typer.Option(
        "v4",
        "--version",
        help="gnomAD version (v3 or v4)"
    ),
    split: bool = typer.Option(
        False,
        "--split/--no-split",
        help="Split downloaded parquet files by variant type"
    ),
    download_workers: Optional[int] = typer.Option(
        None,
        "--download-workers",
        help="Number of workers for parallel downloads"
    ),
    parquet_workers: Optional[int] = typer.Option(
        None,
        "--parquet-workers",
        help="Number of workers for parquet conversion (default: 4)"
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        help="Number of workers for general processing"
    ),
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Timeout in seconds for downloads"
    ),
    run_folder: Optional[str] = typer.Option(
        None,
        "--run-folder",
        help="Optional run folder for pipeline execution"
    ),
    log: bool = typer.Option(
        True,
        "--log/--no-log",
        help="Enable detailed logging to files"
    ),
):
    """
    Download gnomAD VCF files using the Pipelines approach.
    
    Downloads gnomAD data for the specified version.
    """
    if log:
        logs.mkdir(exist_ok=True, parents=True)
        to_nice_file(logs / "prepare_gnomad.json", logs / "prepare_gnomad.log")
        to_nice_stdout()
    
    with start_action(action_type="prepare_gnomad_command") as action:
        action.log(
            message_type="info",
            dest_dir=dest_dir,
            version=version,
            split=split
        )
        
        console.print(f"🔧 Setting up gnomAD {version} pipeline...")
        
        results = PreparationPipelines.download_gnomad(
            dest_dir=Path(dest_dir) if dest_dir else None,
            version=version,
            with_splitting=split,
            download_workers=download_workers,
            parquet_workers=parquet_workers,
            workers=workers,
            log=log,
            timeout=timeout,
            run_folder=run_folder,
        )
        
        console.print(f"✅ gnomAD {version} download completed!")
        action.log(message_type="success", result_keys=list(results.keys()))


@app.command()
def upload_clinvar(
    source_dir: Optional[str] = typer.Option(
        None,
        "--source-dir",
        help="Source directory containing parquet files. If not specified, uses default cache location."
    ),
    repo_id: str = typer.Option(
        "just-dna-seq/clinvar",
        "--repo-id",
        help="Hugging Face repository ID"
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Hugging Face API token. If not provided, uses HF_TOKEN environment variable."
    ),
    pattern: str = typer.Option(
        "**/*.parquet",
        "--pattern",
        help="Glob pattern for finding parquet files"
    ),
    path_prefix: str = typer.Option(
        "data",
        "--path-prefix",
        help="Prefix for paths in the repository"
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        help="Number of parallel workers for uploads"
    ),
    log: bool = typer.Option(
        True,
        "--log/--no-log",
        help="Enable detailed logging to files"
    ),
):
    """
    Upload ClinVar parquet files to Hugging Face Hub.
    
    Only uploads files that differ in size from remote versions, avoiding
    unnecessary data transfers. Files are compared by size before upload.
    
    Requires HF_TOKEN environment variable or --token option for authentication.
    
    Example:
        prepare upload-clinvar --source-dir /path/to/parquet/files
        prepare upload-clinvar --repo-id username/my-dataset
    """
    if log:
        logs.mkdir(exist_ok=True, parents=True)
        to_nice_file(logs / "upload_clinvar.json", logs / "upload_clinvar.log")
        to_nice_stdout()
    
    with start_action(action_type="upload_clinvar_command") as action:
        action.log(
            message_type="info",
            source_dir=source_dir,
            repo_id=repo_id,
            pattern=pattern,
            workers=workers
        )
        
        console.print("🔧 Setting up Hugging Face upload pipeline...")
        console.print(f"📦 Repository: [bold cyan]{repo_id}[/bold cyan]")
        
        if source_dir:
            console.print(f"📁 Source: [bold blue]{source_dir}[/bold blue]")
        else:
            console.print(f"📁 Source: [bold blue]default cache location[/bold blue]")
        
        console.print(f"🔍 Pattern: [bold blue]{pattern}[/bold blue]")
        console.print(f"👷 Workers: [bold blue]{workers or 'auto'}[/bold blue]")
        
        console.print("🚀 Starting upload...")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            task = progress.add_task("Uploading files...", total=None)
            
            results = PreparationPipelines.upload_clinvar_to_hf(
                source_dir=Path(source_dir) if source_dir else None,
                repo_id=repo_id,
                token=token,
                pattern=pattern,
                path_prefix=path_prefix,
                workers=workers,
                log=log,
            )
            
            progress.update(task, description="✅ Upload completed")
        
        # Report results
        console.print("\n✅ Upload process completed!")
        
        uploaded_files = results.get("uploaded_files", [])
        num_uploaded = results.get("num_uploaded", 0)
        num_skipped = results.get("num_skipped", 0)
        
        console.print(f"📊 Summary:")
        console.print(f"  - Total files: [bold]{len(uploaded_files)}[/bold]")
        console.print(f"  - Uploaded: [bold green]{num_uploaded}[/bold green]")
        console.print(f"  - Skipped (size match): [bold yellow]{num_skipped}[/bold yellow]")
        
        action.log(
            message_type="success",
            total=len(uploaded_files),
            uploaded=num_uploaded,
            skipped=num_skipped
        )


@app.command()
def upload_ensembl(
    source_dir: Optional[str] = typer.Option(
        None,
        "--source-dir",
        help="Source directory containing parquet files. If not specified, uses default cache location."
    ),
    repo_id: str = typer.Option(
        "just-dna-seq/ensembl_variations",
        "--repo-id",
        help="Hugging Face repository ID"
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Hugging Face API token. If not provided, uses HF_TOKEN environment variable."
    ),
    pattern: str = typer.Option(
        "**/*.parquet",
        "--pattern",
        help="Glob pattern for finding parquet files"
    ),
    path_prefix: str = typer.Option(
        "data",
        "--path-prefix",
        help="Prefix for paths in the repository"
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        help="Number of parallel workers for uploads"
    ),
    log: bool = typer.Option(
        True,
        "--log/--no-log",
        help="Enable detailed logging to files"
    ),
):
    """
    Upload Ensembl variation parquet files to Hugging Face Hub.
    
    Only uploads files that differ in size from remote versions, avoiding
    unnecessary data transfers. Files are compared by size before upload.
    
    Requires HF_TOKEN environment variable or --token option for authentication.
    
    Example:
        prepare upload-ensembl --source-dir /path/to/parquet/files
        prepare upload-ensembl --repo-id username/my-dataset
    """
    if log:
        logs.mkdir(exist_ok=True, parents=True)
        to_nice_file(logs / "upload_ensembl.json", logs / "upload_ensembl.log")
        to_nice_stdout()
    
    with start_action(action_type="upload_ensembl_command") as action:
        action.log(
            message_type="info",
            source_dir=source_dir,
            repo_id=repo_id,
            pattern=pattern,
            workers=workers
        )
        
        console.print("🔧 Setting up Hugging Face upload pipeline...")
        console.print(f"📦 Repository: [bold cyan]{repo_id}[/bold cyan]")
        
        if source_dir:
            console.print(f"📁 Source: [bold blue]{source_dir}[/bold blue]")
        else:
            console.print(f"📁 Source: [bold blue]default cache location[/bold blue]")
        
        console.print(f"🔍 Pattern: [bold blue]{pattern}[/bold blue]")
        console.print(f"👷 Workers: [bold blue]{workers or 'auto'}[/bold blue]")
        
        console.print("🚀 Starting upload...")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            task = progress.add_task("Uploading files...", total=None)
            
            results = PreparationPipelines.upload_ensembl_to_hf(
                source_dir=Path(source_dir) if source_dir else None,
                repo_id=repo_id,
                token=token,
                pattern=pattern,
                path_prefix=path_prefix,
                workers=workers,
                log=log,
            )
            
            progress.update(task, description="✅ Upload completed")
        
        # Report results
        console.print("\n✅ Upload process completed!")
        
        uploaded_files = results.get("uploaded_files", [])
        num_uploaded = results.get("num_uploaded", 0)
        num_skipped = results.get("num_skipped", 0)
        
        console.print(f"📊 Summary:")
        console.print(f"  - Total files: [bold]{len(uploaded_files)}[/bold]")
        console.print(f"  - Uploaded: [bold green]{num_uploaded}[/bold green]")
        console.print(f"  - Skipped (size match): [bold yellow]{num_skipped}[/bold yellow]")
        
        action.log(
            message_type="success",
            total=len(uploaded_files),
            uploaded=num_uploaded,
            skipped=num_skipped
        )


@app.command()
def version():
    """Show version information."""
    try:
        import importlib.metadata
        version = importlib.metadata.version("genobear")
        console.print(f"genobear version: [bold green]{version}[/bold green]")
    except importlib.metadata.PackageNotFoundError:
        console.print("genobear version: [yellow]development[/yellow]")


if __name__ == "__main__":
    app()

