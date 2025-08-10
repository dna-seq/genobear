"""
Refactored dbSNP database downloader using base classes.
"""

from pathlib import Path
from typing import Dict, Optional, List
from urllib.parse import urljoin
from eliot import start_action
from pydantic import Field

from genobear.config import DEFAULT_DBSNP_URL, DEFAULT_ASSEMBLIES_MAP, DEFAULT_URL_PREFIX
from genobear.databases.vcf_downloader import VCFDatabaseDownloader



class DbSnpDownloader(VCFDatabaseDownloader):
    """dbSNP database downloader for a specific assembly."""

    # Configure via Pydantic fields (no __init__ override)
    database_name: str = Field(default="dbsnp", description="Name of the database")
    base_url: str = Field(default=DEFAULT_DBSNP_URL, description="Base URL for dbSNP downloads")
    assemblies_map: Dict[str, str] = Field(
        default_factory=lambda: DEFAULT_ASSEMBLIES_MAP.copy(),
        description="Assembly version mappings for dbSNP"
    )
    url_prefix: str = Field(default=DEFAULT_URL_PREFIX, description="URL prefix for dbSNP downloads")
    
    def construct_url(
        self, 
        version: Optional[str] = None,
        release: Optional[str] = None,
        **kwargs
    ) -> Optional[str]:
        """
        Construct the dbSNP download URL for this assembly.
        
        Args:
            version: Not used for dbSNP (kept for interface compatibility)
            release: dbSNP release (e.g., 'b156')
            
        Returns:
            Complete URL for the dbSNP file, or None if assembly not supported
        """
        # Use release parameter, fallback to version, then default
        actual_release = release or version or "b156"
        
        with start_action(
            action_type="construct_dbsnp_url",
            assembly=self.assembly,
            base_url=self.base_url,
            release=actual_release
        ) as action:
            
            if self.assembly not in self.assemblies_map:
                action.log(
                    "Assembly not supported", 
                    supported_assemblies=list(self.assemblies_map.keys())
                )
                return None
            
            version_number = self.assemblies_map[self.assembly]
            filename = f"{self.url_prefix}.{version_number}.gz"
            url = urljoin(f"{self.base_url}/{actual_release}/VCF/", filename)
            
            action.add_success_fields(
                constructed_url=url, 
                filename=filename, 
                version_number=version_number,
                release=actual_release
            )
            return url
    
    def download(
        self,
        version: Optional[str] = None,
        release: Optional[str] = None,
        force: bool = False,
        convert_to_parquet_files: bool = True,
        parquet_batch_size: int = 100_000,
        **kwargs
    ) -> Dict[str, Optional[Path]]:
        """
        Download dbSNP files for this assembly.
        
        Args:
            version: Not used for dbSNP (kept for interface compatibility)
            release: dbSNP release (e.g., 'b156')
            force: Whether to force redownload/recreation of files
            convert_to_parquet_files: Whether to convert VCF files to Parquet
            parquet_batch_size: Batch size for Parquet conversion
            
        Returns:
            Dictionary mapping file types to downloaded file paths
        """
        # Use release parameter, fallback to version, then default
        actual_release = release or version or "b156"
        
        with start_action(
            action_type="download_dbsnp",
            assembly=self.assembly,
            release=actual_release,
            convert_to_parquet_files=convert_to_parquet_files
        ) as action:
            
            results = {}
            
            # Construct URL
            url = self.construct_url(release=actual_release)
            if not url:
                action.log("Failed to construct URL for assembly")
                return results
            
            # Determine destination folder
            dest_folder = self.folder_for_version(actual_release)
            dest_folder.mkdir(parents=True, exist_ok=True)
            
            action.log("Starting download", url=url, dest_folder=str(dest_folder))
            
            # Download VCF and index files
            download_results = self.download_vcf_with_index(url, dest_folder, force)
            results.update(download_results)
            
            # Convert to Parquet if requested
            if convert_to_parquet_files and results.get('vcf'):
                from genobear.config import get_parquet_path
                parquet_path = get_parquet_path(self.database_name, self.assembly, actual_release)
                parquet_result = self.convert_to_parquet(
                    results['vcf'],
                    save_parquet=parquet_path,
                    force=force
                )
                if parquet_result:
                    results['parquet'] = parquet_result
            
            # Create default symlink
            release_folder = self.folder_for_version(actual_release)
            self.create_default_symlink(release_folder, "default")
            
            successful_downloads = sum(1 for result in results.values() if result is not None)
            action.add_success_fields(
                total_files=len(results),
                successful_downloads=successful_downloads,
                results={k: str(v) if v else None for k, v in results.items()}
            )
            
            return results


# Convenience functions for backward compatibility
def download_dbsnp(
    assemblies: Optional[List[str]] = None,
    releases: Optional[List[str]] = None,
    output_folder: Optional[Path] = None,
    force: bool = False,
    convert_to_parquet_files: bool = True,
    parquet_batch_size: int = 100_000,
    **kwargs
) -> Dict[str, Optional[Path]]:
    """Download dbSNP for specified assemblies and releases (defaults to [hg38] and [b156])."""
    if assemblies is None:
        assemblies = ["hg38"]
    if releases is None:
        releases = ["b156"]
    
    # Use the multi-assembly function
    results_by_assembly = download_dbsnp_multiple_assemblies(
        assemblies=assemblies,
        releases=releases,
        output_folder=output_folder,
        force=force,
        convert_to_parquet_files=convert_to_parquet_files,
        parquet_batch_size=parquet_batch_size,
        **kwargs
    )
    
    # Flatten results to old format (file path -> file path)
    results = {}
    for assembly_results in results_by_assembly.values():
        for release_results in assembly_results.values():
            for file_type, file_path in release_results.items():
                if file_path:
                    results[str(file_path)] = file_path
    
    return results


def convert_dbsnp_to_parquet(
    assemblies: List[str],
    releases: List[str],
    output_folder: Optional[Path] = None,
    force: bool = False,
    batch_size: int = 100_000
) -> Dict[str, Optional[Path]]:
    """Convert dbSNP VCF files to Parquet format."""
    results = {}
    
    for assembly in assemblies:
        for release in releases:
            downloader = DbSnpDownloader(
                assembly=assembly,
                output_folder=output_folder
            )
            
            # Find VCF files for this assembly/release
            vcf_folder = downloader.folder_for_version(release)
            vcf_files = list(vcf_folder.glob("*.vcf.gz"))
            
            for vcf_file in vcf_files:
                from genobear.config import get_parquet_path
                parquet_path = get_parquet_path(downloader.database_name, downloader.assembly, release)
                parquet_result = downloader.convert_to_parquet(
                    vcf_file,
                    save_parquet=parquet_path,
                    force=force
                )
                if parquet_result:
                    results[str(parquet_result)] = parquet_result
    
    return results


# Multi-assembly convenience function (for backward compatibility)
def download_dbsnp_multiple_assemblies(
    assemblies: List[str],
    releases: List[str],
    output_folder: Optional[Path] = None,
    force: bool = False,
    convert_to_parquet_files: bool = True,
    parquet_batch_size: int = 100_000,
    **kwargs
) -> Dict[str, Dict[str, Optional[Path]]]:
    """
    Download dbSNP for multiple assemblies and releases.
    
    Returns:
        Dictionary mapping assembly to download results
    """
    results = {}
    
    for assembly in assemblies:
        downloader = DbSnpDownloader(
            assembly=assembly,
            output_folder=output_folder,
            **kwargs
        )
        
        assembly_results = {}
        for release in releases:
            release_results = downloader.download(
                release=release,
                force=force,
                convert_to_parquet_files=convert_to_parquet_files,
                parquet_batch_size=parquet_batch_size
            )
            assembly_results[release] = release_results
        
        results[assembly] = assembly_results
    
    return results
