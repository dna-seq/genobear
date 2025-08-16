from __future__ import annotations

from pathlib import Path
from typing import Optional, List
import sys
import os

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from eliot import start_action

from dotenv import load_dotenv


logs = Path("logs") if Path("logs").exists() else Path.cwd().parent / "logs"

load_dotenv()

from genobear.downloaders import EnsemblDownloader, HuggingFaceUploader
from genobear.downloaders.multi_vcf_downloader import DownloadResult
from pycomfort.logging import to_nice_file, to_nice_stdout

# Create the main CLI app
app = typer.Typer(
    name="genobear",
    help="Genomic Database Management and Annotation Tool",
    rich_markup_mode="rich",
    no_args_is_help=True
)

console = Console()

@app.command()
def ensembl(
    cache_subdir: Optional[str] = typer.Option(
        None,
        "--cache-subdir",
        help="Cache subdirectory to save downloaded VCF files. If not specified, uses the default 'ensembl_variations'."
    ),
    chromosomes: Optional[List[str]] = typer.Option(
        None,
        "--chromosomes", "-c",
        help="Specific chromosomes to download (e.g., 1,2,X). If not specified, downloads all available chromosomes"
    ),
    upload_chromosomes: bool = typer.Option(
        False,
        "--upload-chromosomes/--no-upload-chromosomes",
        help="Upload individual chromosome parquet files to HuggingFace dataset."
    ),
    upload_splitted: bool = typer.Option(
        False,
        "--upload-splitted/--no-upload-splitted",
        help="Upload splitted variant type files to HuggingFace dataset. Requires --split to be enabled."
    ),
    hf_token: Optional[str] = typer.Option(
        None,
        "--hf-token",
        help="HuggingFace token for uploading. Can also be set via HF_TOKEN, HUGGINGFACE_HUB_TOKEN, or HUGGING_FACE_HUB_TOKEN environment variable"
    ),
    hf_repo: str = typer.Option(
        "just-dna-seq/ensembl_variations",
        "--hf-repo",
        help="HuggingFace repository to upload to (format: username/repo-name)"
    ),
    force_download: bool = typer.Option(
        False,
        "--force",
        help="Force download even if files already exist"
    ),
    clean_semicolons: bool = typer.Option(
        True,
        "--clean-semicolons/--no-clean-semicolons",
        help="Clean malformed semicolons in VCF files before processing"
    ),
    use_checksums: bool = typer.Option(
        True,
        "--checksums/--no-checksums",
        help="Validate downloaded files using checksums from Ensembl"
    ),
    split: bool = typer.Option(
        False,
        "--split/--no-split",
        help="Split downloaded parquet files by variant type and upload the splitted folder"
    )
):
    """
    Download Ensembl variation VCF files for human genome.
    
    Downloads VCF files from https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/
    and optionally uploads them to HuggingFace dataset. You can upload individual chromosome 
    files using --upload-chromosomes and/or splitted variant type files using --upload-splitted 
    (requires --split to be enabled).
    """
    # Configure logging for this command
    to_nice_file(logs / "download.json", logs / "download.log")
    to_nice_stdout()
    
    with start_action(action_type="download_ensembl_command") as action:
        action.log(
            message_type="info",
            cache_subdir=cache_subdir if cache_subdir else "default",
            chromosomes=chromosomes,
            upload_chromosomes=upload_chromosomes,
            upload_splitted=upload_splitted,
            hf_repo=hf_repo if (upload_chromosomes or upload_splitted) else None,
            force_download=force_download,
            split=split
        )
        
        # Create downloader with custom cache_subdir if specified
        console.print("🔧 Setting up Ensembl downloader...")
        
        downloader_kwargs = {
            "chromosomes": set(chromosomes) if chromosomes else None,
            "clean_semicolons": clean_semicolons,
            "use_checksums": use_checksums,
            "force_download": force_download
        }
        
        if cache_subdir is not None:
            downloader_kwargs["cache_subdir"] = cache_subdir
        
        downloader = EnsemblDownloader(**downloader_kwargs)
        
        # Get the actual cache directory that will be used
        output_dir = downloader.cache_dir
        if cache_subdir is not None:
            console.print(f"📁 Using custom cache subdirectory: [bold blue]{cache_subdir}[/bold blue]")
        else:
            console.print(f"📁 Using default cache subdirectory: [bold blue]ensembl_variations[/bold blue]")
        console.print(f"📁 Cache directory: [bold blue]{output_dir}[/bold blue]")
        
        # Download files
        console.print("📥 Starting download...")
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            download_task = progress.add_task("Downloading VCF files...", total=None)
            
            downloaded_files = downloader.download()
            
            progress.update(download_task, description="✅ Download completed")
        
        console.print(f"✅ Successfully downloaded {len(downloaded_files)} VCF files")
        
        # Split files if requested
        splitted_files = None
        if split:
            console.print("🔄 Splitting parquet files by variant type...")
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True
            ) as progress:
                split_task = progress.add_task("Splitting files...", total=None)
                
                splitted_files = downloader.split_all_variants(explode_snv_alt=True)
                
                progress.update(split_task, description="✅ Split completed")
            
            console.print(f"✅ Successfully split files into variant type directories")
            console.print(f"  📁 Split directory: {downloader.splitted_dir}")
        
        # Upload to HuggingFace if requested
        if upload_chromosomes or upload_splitted:
            uploader = HuggingFaceUploader(
                hf_repo=hf_repo,
                hf_token=hf_token,
                console=console
            )
            
            # Upload splitted files if requested and available
            if upload_splitted:
                if split and downloader.splitted_dir.exists():
                    # Upload splitted folder using upload_folder
                    splitted_folder = downloader.splitted_dir
                    uploader.upload_folder(
                        folder_path=splitted_folder,
                        path_in_repo="splitted_variants",
                        allow_patterns=["*.parquet"],
                        commit_message=f"Upload splitted Ensembl variants by type"
                    )
                else:
                    console.print("⚠️ Cannot upload splitted files: either --split was not enabled or no splitted files were created", style="yellow")
            
            # Upload individual chromosome files if requested
            if upload_chromosomes:
                uploader.upload_to_huggingface(downloaded_files)
        
        action.log(message_type="success", files_downloaded=len(downloaded_files))



@app.command()
def version():
    """Show version information."""
    # Import here to avoid circular imports
    try:
        import importlib.metadata
        version = importlib.metadata.version("genobear")
        console.print(f"genobear version: [bold green]{version}[/bold green]")
    except importlib.metadata.PackageNotFoundError:
        console.print("genobear version: [yellow]development[/yellow]")


if __name__ == "__main__":
    app()
