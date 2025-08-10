#!/usr/bin/env python
"""
GenoBear database download CLI using modular database downloaders.

This module provides a clean Typer-based CLI that delegates to the
specialized database downloader classes in the databases package.
"""

import typer
from typing import List, Optional
from pathlib import Path
from eliot import start_action

from genobear.config import (
    DEFAULT_ASSEMBLY,
    DEFAULT_RELEASE,
    DEFAULT_MAX_CONCURRENT,
    DEFAULT_PARQUET_BATCH_SIZE,
    DEFAULT_ASSEMBLIES_MAP,
    DEFAULT_CLINVAR_ASSEMBLIES_MAP,
    get_database_folder
)

app = typer.Typer(help="Download genomic databases", no_args_is_help=True)


@app.command("dbsnp-latest")
def dbsnp_latest(
    cache_subdir: Optional[Path] = typer.Option(
        None,
        "--cache-subdir",
        help="Cache directory to store the downloaded dbSNP files. Defaults to an OS-specific cache path",
    ),
    base_url: Optional[str] = typer.Option(
        None,
        "--base-url",
        help="Override base URL for dbSNP latest release (advanced). Defaults to NCBI latest release URL",
    ),
    decompress: bool = typer.Option(
        True,
        "--decompress/--no-decompress",
        help="Decompress the VCF after download (default: True)",
    ),
    download_index: bool = typer.Option(
        True,
        "--index/--no-index",
        help="Download the .tbi index file (default: True)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force redownload even if files exist",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose output",
    ),
) -> None:
    """
    Download the latest dbSNP VCF (.gz) and index (.tbi) from NCBI.

    This uses a single-assembly downloader that auto-detects the newest
    GCF_000001405.<version>.gz and matching .tbi from the latest release index.
    """
    with start_action(action_type="download_dbsnp_latest_command") as action:
        try:
            from genobear.downloaders.dbsnp_downloader import DbSNPDownloader

            kwargs = {}
            if cache_subdir is not None:
                kwargs["cache_subdir"] = str(cache_subdir)
            if base_url is not None:
                kwargs["base_url"] = base_url

            if verbose:
                typer.echo("Initializing dbSNP latest downloader...")
                if cache_subdir is not None:
                    typer.echo(f"Cache subdir: {cache_subdir}")
                if base_url is not None:
                    typer.echo(f"Base URL: {base_url}")

            downloader = DbSNPDownloader(**kwargs)

            if verbose:
                typer.echo("Starting download...")

            downloader.fetch_files(
                decompress=decompress,
                download_index=download_index,
                force=force,
            )

            action.add_success_fields(
                vcf=str(downloader.vcf_path),
                tbi=str(downloader.tbi_path) if downloader.tbi_path else None,
            )

            typer.echo(
                typer.style(
                    "✅ dbSNP latest downloaded successfully",
                    fg=typer.colors.GREEN,
                )
            )
            typer.echo(f"  • VCF: {downloader.vcf_path}")
            if downloader.tbi_path:
                typer.echo(f"  • TBI: {downloader.tbi_path}")

        except Exception as e:
            action.log("Error downloading dbSNP latest", error=str(e))
            typer.echo(
                typer.style(
                    f"❌ Error downloading dbSNP latest: {e}",
                    fg=typer.colors.RED,
                ),
                err=True,
            )
            raise typer.Exit(1)

@app.command()
def dbsnp(
    assemblies: List[str] = typer.Option(
        [DEFAULT_ASSEMBLY],
        "--assembly", "-a",
        help=f"List of genome assemblies (e.g., hg19, hg38). Can be specified multiple times. Defaults to {DEFAULT_ASSEMBLY}."
    ),
    output_folder: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output folder for dbSNP files. If not specified, uses default location."
    ),
    releases: List[str] = typer.Option(
        [DEFAULT_RELEASE],
        "--release", "-r",
        help=f"dbSNP releases to download. Can be specified multiple times. Defaults to {DEFAULT_RELEASE}."
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT,
        "--max-concurrent",
        help=f"Maximum concurrent downloads. Default: {DEFAULT_MAX_CONCURRENT}"
    ),
    generate_parquet: bool = typer.Option(
        True,
        "--parquet/--no-parquet",
        help="Generate Parquet files from downloaded VCF data (default: True)"
    ),
    parquet_batch_size: int = typer.Option(
        DEFAULT_PARQUET_BATCH_SIZE,
        "--parquet-batch-size",
        help=f"Batch size for streaming Parquet conversion. Default: {DEFAULT_PARQUET_BATCH_SIZE:,}"
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Force redownload/recreation of files even if they already exist"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output"
    ),
) -> None:
    """
    Download dbSNP database for specified genome assemblies.
    
    This command downloads the latest dbSNP database files for the specified 
    genome assemblies. By default, it downloads release b156.
    
    Examples:
        genobear download dbsnp
        genobear download dbsnp hg19 hg38 --release b156 --parquet
        genobear download dbsnp hg38 --output /path/to/dbsnp --max-concurrent 5 --parquet-batch-size 50000
    """
    with start_action(action_type="download_dbsnp_command",
                     assemblies=assemblies,
                     releases=releases,
                     max_concurrent=max_concurrent,
                     generate_parquet=generate_parquet,
                     parquet_batch_size=parquet_batch_size) as action:
        try:
            # Local import to avoid module import errors when unused
            from genobear.old_databases.dbsnp import download_dbsnp_multiple_assemblies

            # Set default output folder if not provided
            if output_folder is None:
                output_folder = get_database_folder("dbsnp")
                action.log("Using default output folder", folder=str(output_folder))
            
            # Validate assemblies
            unsupported = [a for a in assemblies if a not in DEFAULT_ASSEMBLIES_MAP]
            if unsupported:
                action.log("Unsupported assemblies found", unsupported=unsupported, reason="unsupported_assemblies")
                typer.echo(f"❌ Unsupported assemblies: {', '.join(unsupported)}")
                typer.echo(f"Supported assemblies: {', '.join(DEFAULT_ASSEMBLIES_MAP.keys())}")
                raise typer.Exit(1)
            
            if verbose:
                typer.echo(f"Starting dbSNP download for assemblies: {', '.join(assemblies)}")
                typer.echo(f"Releases: {', '.join(releases)}")
                typer.echo(f"Output folder: {output_folder}")
                typer.echo(f"Max concurrent downloads: {max_concurrent}")
                if generate_parquet:
                    typer.echo(f"Will generate Parquet files (batch size: {parquet_batch_size:,})")
            
            action.log("Starting download process", 
                      output_folder=str(output_folder))
            
            # Download files using modular downloader
            results = download_dbsnp_multiple_assemblies(
                assemblies=assemblies,
                releases=releases,
                output_folder=output_folder,
                max_concurrent=max_concurrent,
                force=force,
                convert_to_parquet_files=generate_parquet,
                parquet_batch_size=parquet_batch_size
            )
            
            # Count successful downloads
            total_files = 0
            successful_files = 0
            for assembly_results in results.values():
                for release_results in assembly_results.values():
                    for result in release_results.values():
                        total_files += 1
                        if result is not None:
                            successful_files += 1
            
            if successful_files == 0:
                action.log("All downloads failed")
                typer.echo(f"❌ All downloads failed")
                raise typer.Exit(1)
            elif successful_files < total_files:
                action.log("Some downloads failed", 
                          successful=successful_files, total=total_files)
                typer.echo(f"⚠️  Partial success: {successful_files}/{total_files} files downloaded")
            
            action.add_success_fields(
                successful_downloads=successful_files,
                total_files=total_files,
                assemblies_processed=assemblies,
                releases_processed=releases
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_files}/{total_files} files for {', '.join(assemblies)}",
                    fg=typer.colors.GREEN
                )
            )
                
        except Exception as e:
            action.log("Error downloading dbSNP", error=str(e))
            typer.echo(
                typer.style(
                    f"❌ Error downloading dbSNP: {e}",
                    fg=typer.colors.RED
                ),
                err=True
            )
            raise typer.Exit(1)


@app.command()
def clinvar(
    assemblies: List[str] = typer.Option(
        [DEFAULT_ASSEMBLY],
        "--assembly", "-a",
        help=f"List of genome assemblies (e.g., hg19, hg38). Can be specified multiple times. Defaults to {DEFAULT_ASSEMBLY}."
    ),
    output_folder: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output folder for ClinVar files. If not specified, uses default from config."
    ),
    dated: bool = typer.Option(
        False,
        "--dated",
        help="Download dated version instead of latest"
    ),
    date_string: Optional[str] = typer.Option(
        None,
        "--date",
        help="Specific date for dated version (e.g., '20250721'). Required if --dated is used."
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT,
        "--max-concurrent",
        help=f"Maximum concurrent downloads. Default: {DEFAULT_MAX_CONCURRENT}"
    ),
    generate_parquet: bool = typer.Option(
        True,
        "--parquet/--no-parquet",
        help="Generate Parquet files from downloaded VCF data (default: True)"
    ),
    parquet_batch_size: int = typer.Option(
        DEFAULT_PARQUET_BATCH_SIZE,
        "--parquet-batch-size",
        help=f"Batch size for streaming Parquet conversion. Default: {DEFAULT_PARQUET_BATCH_SIZE:,}"
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Force redownload/recreation of files even if they already exist"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output"
    ),
) -> None:
    """
    Download ClinVar database for specified genome assemblies.
    
    ClinVar provides a freely accessible, public archive of reports of human genetic 
    variants and their relationships to phenotypes, with supporting evidence.
    
    Examples:
        genobear download clinvar
        genobear download clinvar hg19 hg38 --parquet
        genobear download clinvar hg38 --dated --date 20250721 --parquet
        genobear download clinvar --output /data/clinvar --max-concurrent 5
    """
    with start_action(action_type="download_clinvar_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     generate_parquet=generate_parquet,
                     dated=dated,
                     date_string=date_string) as action:
        try:
            # Use simplified Pooch-based downloader
            from genobear.downloaders.vcf_downloader import (
                download_clinvar_multiple_assemblies,
            )
            # Validate date requirement for dated downloads
            if dated and not date_string:
                action.log("Date string required for dated downloads")
                typer.echo("❌ --date is required when using --dated")
                raise typer.Exit(1)
            
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = get_database_folder("clinvar")
                action.log("Using default output folder", folder=str(output_folder))
            
            # Validate assemblies
            unsupported = [a for a in assemblies if a not in DEFAULT_CLINVAR_ASSEMBLIES_MAP]
            if unsupported:
                action.log("Unsupported assemblies found", unsupported=unsupported)
                typer.echo(f"❌ Unsupported assemblies: {', '.join(unsupported)}")
                typer.echo(f"Supported assemblies: {', '.join(DEFAULT_CLINVAR_ASSEMBLIES_MAP.keys())}")
                raise typer.Exit(1)
            
            if verbose:
                typer.echo(f"Starting ClinVar download for assemblies: {', '.join(assemblies)}")
                typer.echo(f"Output folder: {output_folder}")
                typer.echo(f"Max concurrent downloads: {max_concurrent}")
                typer.echo(f"Download type: {'Dated (' + date_string + ')' if dated else 'Latest'}")
                if generate_parquet:
                    typer.echo(f"Will generate Parquet files (batch size: {parquet_batch_size:,})")
            
            action.log("Starting download process", 
                      output_folder=str(output_folder))
            
            # Download files using modular downloader
            results = download_clinvar_multiple_assemblies(
                assemblies=assemblies,
                output_folder=output_folder,
                dated=dated,
                date_string=date_string,
                max_concurrent=max_concurrent,
                force=force,
                convert_to_parquet_files=generate_parquet,
                parquet_batch_size=parquet_batch_size
            )
            
            # Count successful downloads
            total_files = 0
            successful_files = 0
            for assembly_results in results.values():
                for result in assembly_results.values():
                    total_files += 1
                    if result is not None:
                        successful_files += 1
            
            if successful_files == 0:
                action.log("All downloads failed")
                typer.echo(f"❌ All downloads failed")
                raise typer.Exit(1)
            elif successful_files < total_files:
                action.log("Some downloads failed", 
                          successful=successful_files, total=total_files)
                typer.echo(f"⚠️  Partial success: {successful_files}/{total_files} files downloaded")
            
            action.add_success_fields(
                successful_downloads=successful_files,
                total_files=total_files,
                assemblies_processed=assemblies
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_files}/{total_files} ClinVar files for {', '.join(assemblies)}",
                    fg=typer.colors.GREEN
                )
            )
                
        except Exception as e:
            action.log("Command failed", error=str(e))
            typer.echo(
                typer.style(
                    f"❌ Error downloading ClinVar: {e}",
                    fg=typer.colors.RED
                ),
                err=True
            )
            raise typer.Exit(1)


@app.command()
def annovar(
    assemblies: List[str] = typer.Option(
        [DEFAULT_ASSEMBLY],
        "--assembly", "-a",
        help=f"List of genome assemblies (e.g., hg19, hg38). Can be specified multiple times. Defaults to {DEFAULT_ASSEMBLY}."
    ),
    output_folder: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output folder for ANNOVAR files. If not specified, uses default from config."
    ),
    files: Optional[List[str]] = typer.Option(
        None,
        "--files", "-f",
        help="File patterns to download (e.g., 'refGene', 'clinvar'). Can be specified multiple times. Defaults to minimal set."
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT,
        "--max-concurrent",
        help=f"Maximum concurrent downloads. Default: {DEFAULT_MAX_CONCURRENT}"
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Force redownload/recreation of files even if they already exist"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output"
    ),
) -> None:
    """
    Download ANNOVAR annotation database files for specified genome assemblies.
    
    ANNOVAR is a functional annotation tool for genetic variants detected from 
    diverse genomes. This command downloads annotation databases.
    
    Examples:
        genobear download annovar hg38
        genobear download annovar hg19 hg38 --files refGene clinvar_20230416
        genobear download annovar hg38 --output /data/annovar --max-concurrent 5
    """
    with start_action(action_type="download_annovar_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     files=files) as action:
        try:
            # Local import to avoid module import errors when unused
            from genobear.old_databases.annovar import download_annovar_multiple_assemblies
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = get_database_folder("annovar")
                action.log("Using default output folder", folder=str(output_folder))
            
            # Validate assemblies (ANNOVAR supports many assemblies)
            supported_assemblies = ["hg19", "hg38", "hg18", "mm9", "mm10", "dm3", "dm6", "ce6", "ce10"]
            unsupported = [a for a in assemblies if a not in supported_assemblies]
            if unsupported:
                action.log("Unsupported assemblies found", unsupported=unsupported)
                typer.echo(f"❌ Unsupported assemblies: {', '.join(unsupported)}")
                typer.echo(f"Supported assemblies: {', '.join(supported_assemblies)}")
                raise typer.Exit(1)
            
            if verbose:
                typer.echo(f"Starting ANNOVAR download for assemblies: {', '.join(assemblies)}")
                typer.echo(f"Output folder: {output_folder}")
                typer.echo(f"Max concurrent downloads: {max_concurrent}")
                if files:
                    typer.echo(f"File patterns: {', '.join(files)}")
                else:
                    typer.echo("Using minimal file set (refGene)")
            
            action.log("Starting download process", 
                      output_folder=str(output_folder))
            
            # Download files using modular downloader
            results = download_annovar_multiple_assemblies(
                assemblies=assemblies,
                output_folder=output_folder,
                files=files,
                max_concurrent=max_concurrent,
                force=force
            )
            
            # Count successful downloads
            total_files = 0
            successful_files = 0
            for assembly_results in results.values():
                for result in assembly_results.values():
                    total_files += 1
                    if result is not None:
                        successful_files += 1
            
            if successful_files == 0:
                action.log("All downloads failed")
                typer.echo(f"❌ All downloads failed")
                raise typer.Exit(1)
            elif successful_files < total_files:
                action.log("Some downloads failed", 
                          successful=successful_files, total=total_files)
                typer.echo(f"⚠️  Partial success: {successful_files}/{total_files} files downloaded")
            
            action.add_success_fields(
                successful_downloads=successful_files,
                total_files=total_files,
                assemblies_processed=assemblies
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_files}/{total_files} ANNOVAR files for {', '.join(assemblies)}",
                    fg=typer.colors.GREEN
                )
            )
                
        except Exception as e:
            action.log("Command failed", error=str(e))
            typer.echo(
                typer.style(
                    f"❌ Error downloading ANNOVAR: {e}",
                    fg=typer.colors.RED
                ),
                err=True
            )
            raise typer.Exit(1)


@app.command()
def refseq(
    assemblies: List[str] = typer.Option(
        [DEFAULT_ASSEMBLY],
        "--assembly", "-a",
        help=f"List of genome assemblies (e.g., hg19, hg38). Can be specified multiple times. Defaults to {DEFAULT_ASSEMBLY}."
    ),
    output_folder: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output folder for RefSeq files. If not specified, uses default from config."
    ),
    files: Optional[List[str]] = typer.Option(
        None,
        "--files", "-f",
        help="RefSeq files to download. Can be specified multiple times. Defaults to ncbiRefSeq.txt and ncbiRefSeqLink.txt."
    ),
    format_to_bed: bool = typer.Option(
        True,
        "--format-bed/--no-format-bed",
        help="Format ncbiRefSeq.txt to BED format"
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT,
        "--max-concurrent",
        help=f"Maximum concurrent downloads. Default: {DEFAULT_MAX_CONCURRENT}"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output"
    ),
) -> None:
    """
    Download RefSeq gene annotation files for specified genome assemblies.
    
    RefSeq provides a comprehensive, integrated, non-redundant set of sequences
    including genomic DNA, transcripts, and proteins for major research organisms.
    
    Examples:
        genobear download refseq hg38
        genobear download refseq hg19 hg38 --no-format-bed
        genobear download refseq hg38 --files ncbiRefSeq.txt ncbiRefSeqLink.txt --output /data/refseq
    """
    with start_action(action_type="download_refseq_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     files=files,
                     format_to_bed=format_to_bed) as action:
        try:
            # Local import to avoid module import errors when unused
            from genobear.old_databases.refseq import download_refseq_multiple_assemblies
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = get_database_folder("refseq")
                action.log("Using default output folder", folder=str(output_folder))
            
            # Validate assemblies (RefSeq supports many assemblies)
            supported_assemblies = ["hg19", "hg38", "mm9", "mm10", "dm3", "dm6", "ce6", "ce10", "rn4", "rn5", "rn6"]
            unsupported = [a for a in assemblies if a not in supported_assemblies]
            if unsupported:
                action.log("Unsupported assemblies found", unsupported=unsupported)
                typer.echo(f"❌ Unsupported assemblies: {', '.join(unsupported)}")
                typer.echo(f"Supported assemblies: {', '.join(supported_assemblies)}")
                raise typer.Exit(1)
            
            if verbose:
                typer.echo(f"Starting RefSeq download for assemblies: {', '.join(assemblies)}")
                typer.echo(f"Output folder: {output_folder}")
                typer.echo(f"Max concurrent downloads: {max_concurrent}")
                typer.echo(f"Format to BED: {format_to_bed}")
                if files:
                    typer.echo(f"Files: {', '.join(files)}")
                else:
                    typer.echo("Using default files (ncbiRefSeq.txt, ncbiRefSeqLink.txt)")
            
            action.log("Starting download process", 
                      output_folder=str(output_folder))
            
            # Download files using modular downloader
            results = download_refseq_multiple_assemblies(
                assemblies=assemblies,
                output_folder=output_folder,
                files=files,
                max_concurrent=max_concurrent,
                format_to_bed=format_to_bed
            )
            
            # Count successful downloads
            total_files = 0
            successful_files = 0
            for assembly_results in results.values():
                for result in assembly_results.values():
                    total_files += 1
                    if result is not None:
                        successful_files += 1
            
            if successful_files == 0:
                action.log("All downloads failed")
                typer.echo(f"❌ All downloads failed")
                raise typer.Exit(1)
            elif successful_files < total_files:
                action.log("Some downloads failed", 
                          successful=successful_files, total=total_files)
                typer.echo(f"⚠️  Partial success: {successful_files}/{total_files} files downloaded")
            
            action.add_success_fields(
                successful_downloads=successful_files,
                total_files=total_files,
                assemblies_processed=assemblies
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_files}/{total_files} RefSeq files for {', '.join(assemblies)}",
                    fg=typer.colors.GREEN
                )
            )
                
        except Exception as e:
            action.log("Command failed", error=str(e))
            typer.echo(
                typer.style(
                    f"❌ Error downloading RefSeq: {e}",
                    fg=typer.colors.RED
                ),
                err=True
            )
            raise typer.Exit(1)


@app.command()
def exomiser(
    assemblies: List[str] = typer.Option(
        [DEFAULT_ASSEMBLY],
        "--assembly", "-a",
        help=f"List of genome assemblies (e.g., hg19, hg38). Can be specified multiple times. Defaults to {DEFAULT_ASSEMBLY}."
    ),
    output_folder: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output folder for Exomiser files. If not specified, uses default from config."
    ),
    exomiser_release: Optional[str] = typer.Option(
        None,
        "--exomiser-release", "-r",
        help="Exomiser release version (e.g., '2402'). If not specified, uses latest."
    ),
    phenotype_release: Optional[str] = typer.Option(
        None,
        "--phenotype-release", "-p",
        help="Phenotype release version. If not specified, uses same as exomiser-release."
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT,
        "--max-concurrent",
        help=f"Maximum concurrent downloads. Default: {DEFAULT_MAX_CONCURRENT}"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output"
    ),
) -> None:
    """
    Download Exomiser database files for variant prioritization.
    
    Exomiser is a tool that finds potential disease-causing variants from whole-exome 
    or whole-genome sequencing data. This command downloads the required databases.
    
    Examples:
        genobear download exomiser hg38
        genobear download exomiser hg19 hg38 --exomiser-release 2402
        genobear download exomiser hg38 --output /data/exomiser --max-concurrent 5
    """
    with start_action(action_type="download_exomiser_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     exomiser_release=exomiser_release,
                     phenotype_release=phenotype_release) as action:
        try:
            # Local import to avoid module import errors when unused
            from genobear.old_databases.exomiser import download_exomiser_multiple_assemblies
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = get_database_folder("exomiser")
                action.log("Using default output folder", folder=str(output_folder))
            
            # Validate assemblies (Exomiser supports limited assemblies)
            supported_assemblies = ["hg19", "hg38"]
            unsupported = [a for a in assemblies if a not in supported_assemblies]
            if unsupported:
                action.log("Unsupported assemblies found", unsupported=unsupported)
                typer.echo(f"❌ Unsupported assemblies: {', '.join(unsupported)}")
                typer.echo(f"Supported assemblies: {', '.join(supported_assemblies)}")
                raise typer.Exit(1)
            
            if verbose:
                typer.echo(f"Starting Exomiser download for assemblies: {', '.join(assemblies)}")
                typer.echo(f"Output folder: {output_folder}")
                typer.echo(f"Max concurrent downloads: {max_concurrent}")
                typer.echo(f"Exomiser release: {exomiser_release or 'latest'}")
                typer.echo(f"Phenotype release: {phenotype_release or 'same as exomiser'}")
            
            action.log("Starting download process", 
                      output_folder=str(output_folder))
            
            # Download files using modular downloader
            results = download_exomiser_multiple_assemblies(
                assemblies=assemblies,
                output_folder=output_folder,
                exomiser_release=exomiser_release,
                phenotype_release=phenotype_release,
                max_concurrent=max_concurrent
            )
            
            # Count successful downloads
            successful_assemblies = 0
            for assembly, assembly_results in results.items():
                if any(result is not None for result in assembly_results.values()):
                    successful_assemblies += 1
            
            if successful_assemblies == 0:
                action.log("All downloads failed")
                typer.echo(f"❌ All downloads failed")
                raise typer.Exit(1)
            elif successful_assemblies < len(assemblies):
                action.log("Some downloads failed", 
                          successful=successful_assemblies, total=len(assemblies))
                typer.echo(f"⚠️  Partial success: {successful_assemblies}/{len(assemblies)} assemblies downloaded")
            
            action.add_success_fields(
                successful_assemblies=successful_assemblies,
                total_assemblies=len(assemblies),
                assemblies_processed=assemblies
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded Exomiser files for {successful_assemblies}/{len(assemblies)} assemblies",
                    fg=typer.colors.GREEN
                )
            )
            
            # Show configuration files created
            for assembly, assembly_results in results.items():
                if assembly_results.get('properties'):
                    typer.echo(f"  • Configuration: {assembly_results['properties']}")
                
        except Exception as e:
            action.log("Command failed", error=str(e))
            typer.echo(
                typer.style(
                    f"❌ Error downloading Exomiser: {e}",
                    fg=typer.colors.RED
                ),
                err=True
            )
            raise typer.Exit(1)


@app.command()
def hgmd(
    hgmd_file: Path = typer.Argument(
        ...,
        help="Path to input HGMD VCF file (license required from HGMD)"
    ),
    assembly: str = typer.Argument(
        ...,
        help="Genome assembly (e.g., hg38, hg19)"
    ),
    output_folder: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output folder for converted files. If not specified, uses default from config."
    ),
    output_basename: Optional[str] = typer.Option(
        None,
        "--basename", "-b",
        help="Base name for output files. If not specified, derives from input filename."
    ),
    to_parquet: bool = typer.Option(
        True,
        "--parquet/--no-parquet",
        help="Generate Parquet files"
    ),
    to_tsv: bool = typer.Option(
        True,
        "--tsv/--no-tsv",
        help="Generate TSV files"
    ),
    batch_size: int = typer.Option(
        DEFAULT_PARQUET_BATCH_SIZE,
        "--batch-size",
        help=f"Batch size for streaming conversion. Default: {DEFAULT_PARQUET_BATCH_SIZE:,}"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output"
    ),
) -> None:
    """
    Convert HGMD VCF file to Parquet and/or TSV formats.
    
    HGMD (Human Gene Mutation Database) requires a license. This command converts
    an existing HGMD VCF file to more accessible formats using streaming processing.
    
    Examples:
        genobear download hgmd /path/to/hgmd.vcf.gz hg38
        genobear download hgmd /path/to/hgmd.vcf.gz hg38 --no-tsv --basename hgmd_2024
        genobear download hgmd /path/to/hgmd.vcf.gz hg38 --output /data/hgmd
    """
    with start_action(action_type="convert_hgmd_command",
                     hgmd_file=str(hgmd_file),
                     assembly=assembly,
                     to_parquet=to_parquet,
                     to_tsv=to_tsv) as action:
        try:
            # Local import to avoid module import errors when unused
            from genobear.old_databases.hgmd import convert_hgmd_to_formats
            # Validate input file
            if not hgmd_file.exists():
                action.log("Input HGMD file does not exist")
                typer.echo(f"❌ Input file does not exist: {hgmd_file}")
                raise typer.Exit(1)
            
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = get_database_folder("hgmd")
                action.log("Using default output folder", folder=str(output_folder))
            
            # Validate assembly
            supported_assemblies = list(DEFAULT_ASSEMBLIES_MAP.keys())
            if assembly not in supported_assemblies:
                action.log("Unsupported assembly", assembly=assembly)
                typer.echo(f"❌ Unsupported assembly: {assembly}")
                typer.echo(f"Supported assemblies: {', '.join(supported_assemblies)}")
                raise typer.Exit(1)
            
            if verbose:
                typer.echo(f"Converting HGMD file: {hgmd_file}")
                typer.echo(f"Assembly: {assembly}")
                typer.echo(f"Output folder: {output_folder}")
                typer.echo(f"Generate Parquet: {to_parquet}")
                typer.echo(f"Generate TSV: {to_tsv}")
                typer.echo(f"Batch size: {batch_size:,}")
            
            action.log("Starting conversion process", 
                      output_folder=str(output_folder),
                      input_size=hgmd_file.stat().st_size)
            
            # Convert files using modular function
            results = convert_hgmd_to_formats(
                hgmd_vcf_path=hgmd_file,
                output_folder=output_folder,
                assembly=assembly,
                output_basename=output_basename,
                to_parquet=to_parquet,
                to_tsv=to_tsv,
                batch_size=batch_size
            )
            
            # Check results
            failed_conversions = [fmt for fmt, result in results.items() if result is None]
            if failed_conversions:
                action.log("Some conversions failed", failed_formats=failed_conversions)
                typer.echo(f"❌ Failed conversions: {', '.join(failed_conversions)}")
                if len(failed_conversions) == len(results):
                    raise typer.Exit(1)
            
            # Show successful conversions
            successful_conversions = {fmt: path for fmt, path in results.items() if path is not None}
            if successful_conversions:
                action.add_success_fields(
                    successful_conversions=list(successful_conversions.keys()),
                    output_files={fmt: str(path) for fmt, path in successful_conversions.items()}
                )
                
                typer.echo(
                    typer.style(
                        f"✅ Successfully converted HGMD to {len(successful_conversions)} format(s)",
                        fg=typer.colors.GREEN
                    )
                )
                
                for fmt, path in successful_conversions.items():
                    typer.echo(f"  • {fmt.upper()}: {path}")
            else:
                typer.echo(
                    typer.style(
                        "❌ No conversions were successful",
                        fg=typer.colors.RED
                    )
                )
                raise typer.Exit(1)
                
        except Exception as e:
            action.log("Command failed", error=str(e))
            typer.echo(
                typer.style(
                    f"❌ Error converting HGMD: {e}",
                    fg=typer.colors.RED
                ),
                err=True
            )
            raise typer.Exit(1)


@app.command()
def list_releases() -> None:
    """
    List available dbSNP releases.
    
    Shows commonly used dbSNP release versions.
    """
    with start_action(action_type="list_dbsnp_releases") as action:
        releases = ["b156 (latest)", "b155", "b154", "b153"]
        action.log("Listing available dbSNP releases", releases=releases)
        
        typer.echo("Common dbSNP releases:")
        typer.echo("  • b156 (latest)")
        typer.echo("  • b155")
        typer.echo("  • b154")
        typer.echo("  • b153")
        typer.echo("\nNote: Use 'b156' for the latest release")
        
        action.add_success_fields(releases_listed=len(releases))


@app.command()
def list_assemblies() -> None:
    """
    List supported genome assemblies.
    """
    with start_action(action_type="list_supported_assemblies") as action:
        action.log("Listing supported assemblies")
        
        typer.echo("Supported genome assemblies:")
        for assembly in DEFAULT_ASSEMBLIES_MAP.keys():
            typer.echo(f"  • {assembly}")
        
        action.add_success_fields(
            assemblies_count=len(DEFAULT_ASSEMBLIES_MAP),
            assemblies=list(DEFAULT_ASSEMBLIES_MAP.keys())
        )


@app.command()
def list_databases() -> None:
    """
    List available databases for download.
    """
    with start_action(action_type="list_available_databases") as action:
        databases = [
            ("dbsnp", "dbSNP - Single Nucleotide Polymorphism database", "Versioned releases (b156, b155, etc.)"),
            ("clinvar", "ClinVar - Clinical significance of genomic variations", "Latest or dated versions"),
            ("annovar", "ANNOVAR - Functional annotation of genetic variants", "Multiple annotation databases"),
            ("hgmd", "HGMD - Human Gene Mutation Database (license required)", "VCF conversion to Parquet/TSV"),
            ("refseq", "RefSeq - Reference sequence database", "Gene annotations with BED formatting"),
            ("exomiser", "Exomiser - Variant prioritization tool", "Phenotype and variant databases"),
        ]
        
        typer.echo("Available databases:")
        for name, description, versions in databases:
            typer.echo(f"\n  🧬 {name}")
            typer.echo(f"     {description}")
            typer.echo(f"     {versions}")
        
        typer.echo(f"\nUsage examples:")
        typer.echo(f"  genobear download dbsnp hg38 --parquet")
        typer.echo(f"  genobear download clinvar hg38 --parquet")
        typer.echo(f"  genobear download clinvar hg38 --dated --date 20250721")
        typer.echo(f"  genobear download annovar hg38 --files refGene clinvar")
        typer.echo(f"  genobear download hgmd /path/to/hgmd.vcf.gz hg38")
        typer.echo(f"  genobear download refseq hg38 --format-bed")
        typer.echo(f"  genobear download exomiser hg38 --exomiser-release 2402")
        
        action.add_success_fields(databases_count=len(databases))


if __name__ == "__main__":
    app()