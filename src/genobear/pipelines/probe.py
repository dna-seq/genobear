#!/usr/bin/env python3
"""
Probe script to test and run the Ensembl VCF download pipeline.

This script demonstrates how to use the genobear Pipelines to download
and process Ensembl VCF data in two ways:
1. Using Pipelines.download_ensembl() for convenience
2. Using Pipelines.ensembl() + Pipelines.execute() for custom control
"""

from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
from eliot import start_action
from pycomfort.logging import to_nice_stdout, to_nice_file

from genobear import Pipelines

app = typer.Typer(help="Ensembl VCF Download Pipeline Probe")


@app.command()
def main(
    download_dir: Optional[Path] = typer.Option(
        None,
        "--download-dir",
        "-d",
        help="Destination for downloads. If omitted, internal cache resolution is used."
    ),
    with_splitting: bool = typer.Option(
        True,
        "--with-splitting/--no-splitting",
        help="Whether to enable variant splitting"
    ),
    env_file: Optional[Path] = typer.Option(
        None,
        "--env-file",
        help="Path to .env file to load environment variables from"
    ),
    run_folder: Optional[str] = typer.Option(
        None,
        "--run-folder",
        help="Optional run folder for pipeline execution (None by default)"
    )
) -> None:
    """Main function to run the Ensembl download pipeline."""
    # Setup logging first
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    json_log_path = logs_dir / "probe_ensembl.json"
    rendered_log_path = logs_dir / "probe_ensembl.log"
    
    # Setup nice logging to both files and stdout
    to_nice_file(json_log_path, rendered_log_path)
    to_nice_stdout()
    
    # Load environment variables
    if env_file and env_file.exists():
        load_dotenv(env_file)
    else:
        load_dotenv()  # Load from default .env file if it exists
    
    with start_action(action_type="probe_ensembl_pipeline") as action:
        action.log(message_type="pipeline_start", message="Starting Ensembl VCF Download Pipeline Probe")
        # If the user provided a destination directory, ensure it exists
        if download_dir is not None:
            download_dir.mkdir(parents=True, exist_ok=True)
            action.log(message_type="download_dir_configured", message="Download directory configured", download_dir=str(download_dir))
        
        # Option 1: Use the Pipelines convenience method
        action.log(message_type="ensembl_download_start", message="Running Ensembl download using Pipelines")
        results = Pipelines.download_ensembl(
            dest_dir=download_dir,
            with_splitting=with_splitting,
            run_folder=run_folder
            # Note: The Ensembl pipeline has default URL and pattern set in Pipelines
        )
        
        action.log(message_type="ensembl_download_complete", message="Ensembl download completed successfully")
        action.log(message_type="results_summary", message="Results summary", result_keys=list(results.keys()))
        
        # Log information about downloaded files
        if "vcf_parquet_path" in results:
            parquet_result = results["vcf_parquet_path"]
            parquet_out = parquet_result.output
            
            # Extract parquet paths
            if hasattr(parquet_out, "ravel"):
                parquet_seq = parquet_out.ravel().tolist()
            elif isinstance(parquet_out, list):
                parquet_seq = parquet_out
            else:
                parquet_seq = [parquet_out]
            
            parquet_paths = [p if isinstance(p, Path) else Path(p) for p in parquet_seq]
            
            action.log(message_type="parquet_files_generated", message="Generated parquet files", file_count=len(parquet_paths))
            for parquet_path in parquet_paths:
                if parquet_path.exists():
                    size_mb = parquet_path.stat().st_size / (1024 * 1024)
                    action.log(message_type="parquet_file_created", 
                             message="Parquet file created",
                             path=str(parquet_path), 
                             size_mb=round(size_mb, 1))
                else:
                    action.log(message_type="parquet_file_missing", message="Parquet file not found", path=str(parquet_path))
        
        if "vcf_lazy_frame" in results:
            lazy_frame_result = results["vcf_lazy_frame"]
            lazy_frame_out = lazy_frame_result.output
            
            # Get schema information from the lazy frames
            if hasattr(lazy_frame_out, "ravel"):
                lazy_frames = lazy_frame_out.ravel().tolist()
            elif isinstance(lazy_frame_out, list):
                lazy_frames = lazy_frame_out
            else:
                lazy_frames = [lazy_frame_out]
            
            action.log(message_type="lazy_frames_generated", message="Generated lazy frames", frame_count=len(lazy_frames))
            for i, lf in enumerate(lazy_frames):
                if hasattr(lf, 'schema'):
                    action.log(message_type="lazy_frame_schema", 
                             message="LazyFrame schema",
                             frame_index=i+1, 
                             columns=list(lf.schema.keys()))
                else:
                    action.log(message_type="lazy_frame_info", 
                             message="LazyFrame info",
                             frame_index=i+1, 
                             type=str(type(lf)))
        
        # Note: If you need lower-level control, construct a pipeline via Pipelines.ensembl()
        # and execute it with Pipelines.execute() in your own scripts.


if __name__ == "__main__":
    app()
