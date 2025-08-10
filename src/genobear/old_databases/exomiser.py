"""
Exomiser database download functions.
"""

from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
from eliot import start_action
from pydantic import Field

from genobear.config import DEFAULT_EXOMISER_URL
from genobear.databases.base import ArchiveDatabaseDownloader


class ExomiserDownloader(ArchiveDatabaseDownloader):
    """Exomiser database downloader."""

    # Configure via Pydantic fields (no __init__ override)
    base_url: str = Field(default=DEFAULT_EXOMISER_URL, description="Base URL for Exomiser downloads")

    @property
    def database_name(self) -> str:
        return "exomiser"

    def download(
        self,
        exomiser_release: Optional[str] = None,
        phenotype_release: Optional[str] = None,
        force: bool = False
    ) -> Dict[str, Optional[Path]]:
        """Download Exomiser data for a specific assembly."""
        exomiser_release = exomiser_release or "2402"
        phenotype_release = phenotype_release or exomiser_release

        with start_action(
            action_type="download_exomiser_assembly",
            assembly=self.assembly,
            exomiser_release=exomiser_release,
            phenotype_release=phenotype_release,
        ) as action:
            results: Dict[str, Optional[Path]] = {}
            output_folder = self.output_folder_path
            output_folder.mkdir(parents=True, exist_ok=True)

            # Download and extract phenotype data (shared)
            phenotype_path = self._download_phenotype_data(
                output_folder, phenotype_release, force
            )
            if phenotype_path:
                results["phenotype"] = phenotype_path

            # Download and extract assembly-specific data
            assembly_path = self._download_assembly_data(
                output_folder, exomiser_release, force
            )
            if assembly_path:
                results["assembly_data"] = assembly_path

            # Create application.properties file
            properties_path = self._create_application_properties(
                output_folder, exomiser_release, phenotype_release
            )
            if properties_path:
                results["properties"] = properties_path

            action.add_success_fields(
                results={k: str(v) for k, v in results.items() if v}
            )
            return results

    def _download_phenotype_data(
        self, output_folder: Path, release: str, force: bool
    ) -> Optional[Path]:
        """Download and extract phenotype data."""
        filename = f"{release}_phenotype.zip"
        url = urljoin(f"{self.base_url}/data/", filename)
        path = output_folder / filename
        extracted_folder = output_folder / f"{release}_phenotype"

        if not extracted_folder.exists() or force:
            if not path.exists() or force:
                self.download_file(url, path)
            if path.exists():
                self.extract_archive(path, output_folder)
                return extracted_folder
        return extracted_folder if extracted_folder.exists() else None

    def _download_assembly_data(
        self, output_folder: Path, release: str, force: bool
    ) -> Optional[Path]:
        """Download and extract assembly-specific data."""
        exomiser_assembly = self._get_exomiser_assembly_name()
        filename = f"{release}_{exomiser_assembly}.zip"
        url = urljoin(f"{self.base_url}/data/", filename)
        path = output_folder / filename
        extracted_folder = output_folder / f"{release}_{exomiser_assembly}"

        if not extracted_folder.exists() or force:
            if not path.exists() or force:
                self.download_file(url, path)
            if path.exists():
                self.extract_archive(path, output_folder)
                return extracted_folder
        return extracted_folder if extracted_folder.exists() else None

    def _create_application_properties(
        self, output_folder: Path, exomiser_release: str, phenotype_release: str
    ) -> Optional[Path]:
        """Create the application.properties file."""
        assembly_folder = self.assembly_folder
        assembly_folder.mkdir(parents=True, exist_ok=True)
        properties_file = assembly_folder / "application.properties"
        exomiser_assembly = self._get_exomiser_assembly_name()

        content = f"""
exomiser.data-directory={output_folder}
exomiser.{self.assembly}.data-version={exomiser_release}
exomiser.phenotype.data-version={phenotype_release}
exomiser.{self.assembly}.transcript-source=refseq
exomiser.{self.assembly}.variant-white-list-path={exomiser_release}_{self.assembly}_clinvar_whitelist.tsv.gz
exomiser.{self.assembly}.remm-path=${{exomiser.data-directory}}/{exomiser_release}_{exomiser_assembly}_remm.tsv.gz
exomiser.{self.assembly}.cadd-snv-path=${{exomiser.data-directory}}/{exomiser_release}_{exomiser_assembly}_cadd_snv.tsv.gz
exomiser.{self.assembly}.cadd-indel-path=${{exomiser.data-directory}}/{exomiser_release}_{exomiser_assembly}_cadd_indel.tsv.gz
"""
        with open(properties_file, "w") as f:
            f.write(content.strip())
        return properties_file

    def _get_exomiser_assembly_name(self) -> str:
        """Get the Exomiser-compatible assembly name."""
        if self.assembly == "hg19":
            return "GRCh37"
        if self.assembly == "hg38":
            return "GRCh38"
        return self.assembly


def download_exomiser_multiple_assemblies(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    base_url: str = DEFAULT_EXOMISER_URL,
    exomiser_release: Optional[str] = None,
    phenotype_release: Optional[str] = None,
    max_concurrent: int = 3,
    force: bool = False,
    **kwargs,
) -> Dict[str, Dict[str, Optional[Path]]]:
    """Download Exomiser files for multiple assemblies."""
    results = {}
    with start_action(
        action_type="download_exomiser_files",
        assemblies=assemblies,
        output_folder=str(output_folder),
    ):
        for assembly in assemblies:
            downloader = ExomiserDownloader(
                assembly=assembly,
                base_url=base_url,
                output_folder=output_folder,
                max_concurrent=max_concurrent,
            )
            results[assembly] = downloader.download(
                exomiser_release=exomiser_release,
                phenotype_release=phenotype_release,
                force=force,
            )
    return results

def download_exomiser(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    base_url: str = DEFAULT_EXOMISER_URL,
    exomiser_release: Optional[str] = None,
    phenotype_release: Optional[str] = None,
    max_concurrent: int = 3
) -> Dict[str, Optional[Path]]:
    """Legacy wrapper for downloading Exomiser files."""
    results_by_assembly = download_exomiser_multiple_assemblies(
        assemblies=assemblies,
        output_folder=output_folder,
        base_url=base_url,
        exomiser_release=exomiser_release,
        phenotype_release=phenotype_release,
        max_concurrent=max_concurrent,
    )
    
    # Flatten results for backward compatibility
    flat_results = {}
    for assembly_results in results_by_assembly.values():
        flat_results.update(assembly_results)
        
    return flat_results
