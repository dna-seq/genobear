"""
GenoBear - Genomic Database Management and Annotation Tool

A unified toolkit for downloading, converting, and annotating genomic databases.
"""

import typer
from eliot import start_action, to_file
from pathlib import Path

# Import main CLI apps
from genobear.download import app as download_app
from genobear.annotate import app as annotate_app

# Import API functions for programmatic use
from genobear.api import (
    # Database download functions (sync)
    download_dbsnp_sync, download_clinvar_sync, download_annovar_sync,
    download_refseq_sync, download_exomiser_sync,
    
    # Conversion functions
    convert_dbsnp_to_parquet_sync, convert_clinvar_to_parquet_sync,
    convert_hgmd_to_formats,
    
    # Annotation functions
    annotate_vcf, annotate_vcf_batch,
    
    # Discovery functions
    discover_databases, get_available_assemblies, get_available_releases,
    
    # Configuration functions
    get_database_folder, get_parquet_path, get_vcf_path,
    list_supported_databases,
    
    # Convenience functions
    download_and_convert_dbsnp, download_and_convert_clinvar,
    
    # Constants
    DEFAULT_ASSEMBLY, DEFAULT_RELEASE, SUPPORTED_DATABASES
)

from genobear.config import DEFAULT_LOGS_FOLDER

# Version will be set dynamically by uv/hatch
__version__ = "0.1.0"

# Set up logging
DEFAULT_LOGS_FOLDER.mkdir(parents=True, exist_ok=True)
log_file = DEFAULT_LOGS_FOLDER / "genobear.log"
to_file(open(log_file, "w"))

# Create main CLI app
app = typer.Typer(
    help="GenoBear - Genomic Database Management and Annotation Tool",
    no_args_is_help=True
)

# Add sub-commands
app.add_typer(download_app, name="download", help="Download genomic databases")
app.add_typer(annotate_app, name="annotate", help="Annotate VCF files with genomic databases")

# Main command functions for version and info
@app.command()
def version():
    """Show GenoBear version."""
    typer.echo(f"GenoBear version {__version__}")

@app.command()
def info():
    """Show GenoBear information and configuration."""
    with start_action(action_type="show_info") as action:
        typer.echo("🧬 GenoBear - Genomic Database Management and Annotation Tool")
        typer.echo(f"Version: {__version__}")
        typer.echo()
        typer.echo("📁 Default Directories:")
        typer.echo(f"  • Databases: {get_database_folder('dbsnp').parent}")
        typer.echo(f"  • Logs: {DEFAULT_LOGS_FOLDER}")
        typer.echo()
        typer.echo("🗃️  Supported Databases:")
        for db_type, description in SUPPORTED_DATABASES.items():
            typer.echo(f"  • {db_type}: {description}")
        typer.echo()
        typer.echo("🔧 Default Assembly: " + DEFAULT_ASSEMBLY)
        typer.echo("🔧 Default dbSNP Release: " + DEFAULT_RELEASE)
        
        action.add_success_fields(version=__version__)

# Export main API functions
__all__ = [
    # Main CLI app
    "app",
    
    # Database download functions
    "download_dbsnp_sync", "download_clinvar_sync", "download_annovar_sync",
    "download_refseq_sync", "download_exomiser_sync",
    
    # Conversion functions
    "convert_dbsnp_to_parquet_sync", "convert_clinvar_to_parquet_sync",
    "convert_hgmd_to_formats",
    
    # Annotation functions
    "annotate_vcf", "annotate_vcf_batch",
    
    # Discovery functions
    "discover_databases", "get_available_assemblies", "get_available_releases",
    
    # Configuration functions
    "get_database_folder", "get_parquet_path", "get_vcf_path",
    "list_supported_databases",
    
    # Convenience functions
    "download_and_convert_dbsnp", "download_and_convert_clinvar",
    
    # Constants
    "DEFAULT_ASSEMBLY", "DEFAULT_RELEASE", "SUPPORTED_DATABASES",
    "__version__"
]
