import typer
from typing import Optional
from eliot import start_action
from pycomfort.logging import to_nice_stdout, to_nice_file
import sys
from pathlib import Path

from genobear.download import app as download_app
from genobear.annotate import app as annotate_app

app = typer.Typer(
    name="genobear",
    help="GenoBear: A powerful CLI tool for genomic data processing",
    add_completion=False,
)

# Add subcommands
app.add_typer(download_app, name="download")
app.add_typer(annotate_app, name="annotate")


@app.callback()
def callback(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
) -> None:
    """
    GenoBear: A powerful CLI tool for genomic data processing.
    
    Provides commands for downloading genomic databases.
    """
    if version:
        typer.echo("genobear 0.1.0")
        raise typer.Exit()
    
    # Set up nice logging to stdout and files
    to_nice_stdout()
    
    # Determine project root and logs directory
    project_root = Path(__file__).resolve().parents[2]
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Define log file paths
    json_log_path = log_dir / "genobear.log.json"
    rendered_log_path = log_dir / "genobear.log"
    
    # Configure file logging
    to_nice_file(output_file=json_log_path, rendered_file=rendered_log_path)
    
    if verbose:
        typer.echo("Verbose mode enabled")
