from __future__ import annotations

import re
from pathlib import Path
import urllib.request
from typing import Optional

from eliot import start_action
import typer
from pydantic import HttpUrl, model_validator

from genobear.downloaders.multi_vcf_downloader import MultiVCFDownloader


class DbSNPDownloader(MultiVCFDownloader):
    """
    Downloader for dbSNP VCF files from the NCBI latest release directory.

    Notes:
    - Assumes a single assembly source (no multi-assembly logic).
    - Automatically discovers the latest available `GCF_000001405.*.gz` and its
      corresponding `.tbi` and `.md5` files from the index page.
    """

    # Defaults; will be set by validator if not provided
    base_url: HttpUrl = None  # type: ignore[assignment]
    vcf_filename: str = None  # type: ignore[assignment]
    tbi_filename: str = None  # type: ignore[assignment]
    hash_filename: Optional[str] = None
    cache_subdir: str = None  # type: ignore[assignment]

    @model_validator(mode="before")
    @classmethod
    def set_dbsnp_defaults(cls, values: dict) -> dict:
        """Set dbSNP-specific defaults and auto-detect latest filenames."""
        with start_action(action_type="dbsnp_set_defaults") as action:
            base_url: Optional[str] = values.get(
                "base_url",
                "https://ftp.ncbi.nih.gov/snp/latest_release/VCF/",
            )
            values["base_url"] = base_url

            # Use a stable cache subdirectory if not provided
            if not values.get("cache_subdir"):
                values["cache_subdir"] = "vcf_downloader/dbsnp"

            # If filenames already provided, keep them as-is
            if values.get("vcf_filename") and values.get("tbi_filename"):
                action.log(message_type="info", filenames_provided=True)
                # Ensure hash filename is set if missing
                if not values.get("hash_filename") and isinstance(values["vcf_filename"], str):
                    values["hash_filename"] = f"{values['vcf_filename']}.md5"
                return values

            # Discover latest version by scraping index listing
            try:
                with urllib.request.urlopen(base_url) as response:
                    index_html = response.read().decode("utf-8", errors="ignore")
            except Exception as error:  # pragma: no cover - network/IO issues
                action.log(message_type="error", error=str(error))
                raise

            # Find all GCF_000001405.<num>.gz entries (exclude .tbi/.md5)
            matches = re.findall(r"GCF_000001405\.(\d+)\.gz(?<!\.tbi)(?<!\.md5)", index_html)

            if not matches:
                raise ValueError("No dbSNP VCF files found at index page")

            # Select the highest available version
            versions = sorted({int(v) for v in matches})
            latest_version = versions[-1]

            vcf_filename = f"GCF_000001405.{latest_version}.gz"
            tbi_filename = f"GCF_000001405.{latest_version}.gz.tbi"
            hash_filename = f"GCF_000001405.{latest_version}.gz.md5"

            # Basic existence check in the listing for the companion files
            if tbi_filename not in index_html:
                raise ValueError(f"Missing index file in listing: {tbi_filename}")
            if hash_filename not in index_html:
                # Hash is optional but recommended; log and continue
                action.log(message_type="warning", missing_hash_for=vcf_filename)
                hash_filename = None

            values["vcf_filename"] = vcf_filename
            values["tbi_filename"] = tbi_filename
            values["hash_filename"] = hash_filename

            action.log(
                message_type="info",
                detected_version=latest_version,
                vcf_filename=vcf_filename,
                tbi_filename=tbi_filename,
                hash_filename=hash_filename,
            )

            return values


app = typer.Typer(help="Download the latest dbSNP VCF and index from NCBI")


@app.command("download")
def cli_download(
    cache_subdir: Optional[Path] = typer.Option(
        None,
        "--cache-subdir",
        help="Cache directory to store dbSNP files. Defaults to OS cache path",
    ),
    base_url: Optional[str] = typer.Option(
        None,
        "--base-url",
        help="Override base URL (advanced). Defaults to NCBI latest release URL",
    ),
    decompress: bool = typer.Option(
        True,
        "--decompress/--no-decompress",
        help="Decompress the VCF after download (default: True)",
    ),
    index: bool = typer.Option(
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
    """Download the latest dbSNP VCF (.gz) and TBI (.tbi)."""
    with start_action(action_type="dbsnp_cli_download") as action:
        kwargs = {}
        if cache_subdir is not None:
            kwargs["cache_subdir"] = str(cache_subdir)
        if base_url is not None:
            kwargs["base_url"] = base_url

        if verbose:
            typer.echo("Initializing dbSNP downloader...")
            if cache_subdir is not None:
                typer.echo(f"Cache subdir: {cache_subdir}")
            if base_url is not None:
                typer.echo(f"Base URL: {base_url}")

        downloader = DbSNPDownloader(**kwargs)

        if verbose:
            typer.echo("Starting download...")

        downloader.fetch_files(
            decompress=decompress,
            download_index=index,
            force=force,
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


if __name__ == "__main__":
    app()


