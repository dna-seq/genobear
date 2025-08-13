from __future__ import annotations

from pathlib import Path
from typing import Optional, List
import sys

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from eliot import start_action

from genobear.downloaders import EnsemblDownloader

# Create the main CLI app
app = typer.Typer(
    name="genobear",
    help="Genomic Database Management and Annotation Tool",
    rich_markup_mode="rich"
)

console = Console()

@app.command()
def download_ensembl(
    output_dir: Path = typer.Option(
        Path("./data/ensembl_variations"),
        "--output-dir", "-o",
        help="Directory to save downloaded VCF files"
    ),
    chromosomes: Optional[List[str]] = typer.Option(
        None,
        "--chromosomes", "-c",
        help="Specific chromosomes to download (e.g., 1,2,X). If not specified, downloads all available chromosomes"
    ),
    upload: bool = typer.Option(
        False,
        "--upload", "-u",
        help="Upload downloaded VCF files to HuggingFace dataset: just-dna-seq/ensembl_variations"
    ),
    hf_token: Optional[str] = typer.Option(
        None,
        "--hf-token",
        help="HuggingFace token for uploading. Can also be set via HF_TOKEN or HUGGINGFACE_HUB_TOKEN environment variable"
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
    )
):
    """
    Download Ensembl variation VCF files for human genome.
    
    Downloads VCF files from https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/
    and optionally uploads them to the HuggingFace dataset.
    """
    with start_action(action_type="download_ensembl_command") as action:
        action.log(
            message_type="info",
            output_dir=str(output_dir),
            chromosomes=chromosomes,
            upload=upload,
            force_download=force_download
        )
        
        try:
            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)
            console.print(f"📁 Output directory: [bold blue]{output_dir}[/bold blue]")
            
            # Create downloader
            console.print("🔧 Setting up Ensembl downloader...")
            downloader = EnsemblDownloader(
                cache_dir=output_dir,
                chromosomes=set(chromosomes) if chromosomes else None,
                clean_semicolons=clean_semicolons,
                use_checksums=use_checksums,
                force_download=force_download
            )
            
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
            
            # Upload to HuggingFace if requested
            if upload:
                _upload_to_huggingface(
                    downloaded_files=downloaded_files,
                    hf_token=hf_token,
                    console=console
                )
            
            action.log(message_type="success", files_downloaded=len(downloaded_files))
            
        except Exception as e:
            action.log(message_type="error", error=str(e))
            console.print(f"❌ Error: {e}", style="red")
            sys.exit(1)


def _upload_to_huggingface(
    downloaded_files: dict,
    hf_token: Optional[str],
    console: Console
) -> None:
    """Upload downloaded VCF files to HuggingFace dataset."""
    
    with start_action(action_type="upload_to_huggingface") as action:
        try:
            from huggingface_hub import HfApi, upload_file
            import os
            
            # Get token from parameter or environment variables
            token = hf_token or os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_HUB_TOKEN")
            if not token:
                console.print("❌ HuggingFace token required for upload. Set --hf-token or HF_TOKEN/HUGGINGFACE_HUB_TOKEN environment variable", style="red")
                return
            
            console.print("🚀 Uploading to HuggingFace: [bold blue]just-dna-seq/ensembl_variations[/bold blue]")
            
            api = HfApi()
            repo_id = "just-dna-seq/ensembl_variations"
            
            # Upload each file
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True
            ) as progress:
                
                upload_task = progress.add_task(f"Uploading {len(downloaded_files)} files...", total=len(downloaded_files))
                
                for identifier, file_path in downloaded_files.items():
                    file_path = Path(file_path)
                    
                    progress.update(upload_task, description=f"Uploading {file_path.name}...")
                    
                    # Upload to the dataset repository
                    upload_file(
                        path_or_fileobj=str(file_path),
                        path_in_repo=f"vcf_files/{file_path.name}",
                        repo_id=repo_id,
                        repo_type="dataset",
                        token=token
                    )
                    
                    action.log(message_type="info", uploaded_file=str(file_path))
                    progress.advance(upload_task)
                
                progress.update(upload_task, description="✅ Upload completed")
            
            console.print(f"✅ Successfully uploaded {len(downloaded_files)} files to HuggingFace")
            console.print(f"🔗 View dataset at: [link]https://huggingface.co/datasets/{repo_id}[/link]")
            
        except ImportError:
            console.print("❌ huggingface_hub not available. Install with: pip install huggingface-hub", style="red")
            action.log(message_type="error", error="huggingface_hub not available")
        except Exception as e:
            console.print(f"❌ Upload failed: {e}", style="red")
            action.log(message_type="error", error=str(e))


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
