"""
ANNOVAR database download functions.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
from eliot import start_action

from genobear.config import DEFAULT_ANNOVAR_URL, get_database_folder
from genobear.databases.base import ArchiveDatabaseDownloader



class AnnovarDownloader(ArchiveDatabaseDownloader):
    """ANNOVAR database downloader."""

    @property
    def database_name(self) -> str:
        return "annovar"

    def download(
        self,
        files: Optional[List[str]] = None,
        force: bool = False,
        **kwargs,
    ) -> Dict[str, Optional[Path]]:
        """Download ANNOVAR annotation files for a specific assembly."""
        with start_action(
            action_type="download_annovar_assembly",
            assembly=self.assembly,
            files=files,
        ) as action:
            results = {}
            assembly_folder = self.assembly_folder
            assembly_folder.mkdir(parents=True, exist_ok=True)

            # Download and parse the database list file
            avdblist_path = self._download_avdblist(assembly_folder)
            if not avdblist_path:
                action.log("Failed to download database list.")
                return results

            available_files = self._parse_avdblist(avdblist_path)
            if not available_files:
                action.log("Failed to parse database list.")
                return results

            # Determine which files to download
            files_to_download = self._filter_files_to_download(available_files, files)
            action.log("Files to download", count=len(files_to_download))

            # Download the files
            download_tasks = [
                (
                    urljoin(f"{self.base_url}/", file_name),
                    assembly_folder / file_name,
                    file_name,
                )
                for file_name in files_to_download
            ]

            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                future_to_key = {
                    executor.submit(self.download_file, url, path): key
                    for url, path, key in download_tasks
                    if not path.exists() or force
                }

                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        results[key] = future.result()
                    except Exception as e:
                        action.log("Download error", file=key, error=str(e))
                        results[key] = None

            return results

    def _download_avdblist(self, assembly_folder: Path) -> Optional[Path]:
        """Download the avdblist.txt file."""
        avdblist_file = f"{self.assembly}_avdblist.txt"
        avdblist_url = urljoin(f"{self.base_url}/", avdblist_file)
        avdblist_path = assembly_folder / avdblist_file
        return self.download_file(avdblist_url, avdblist_path)

    def _parse_avdblist(self, avdblist_path: Path) -> List[str]:
        """Parse the avdblist.txt file."""
        try:
            with open(avdblist_path, "r") as f:
                return [line.strip() for line in f if line.strip()]
        except Exception:
            return []

    def _filter_files_to_download(
        self, available_files: List[str], patterns: Optional[List[str]] = None
    ) -> List[str]:
        """Filter files to download based on given patterns."""
        if not patterns:
            patterns = ["refGene*"]  # Default minimal set

        files_to_download = set()
        for pattern in patterns:
            if "*" in pattern:
                prefix = pattern.replace("*", "")
                files_to_download.update(
                    f for f in available_files if f.startswith(prefix)
                )
            else:
                if pattern in available_files:
                    files_to_download.add(pattern)

        return list(files_to_download)


def download_annovar_multiple_assemblies(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
    base_url: str = DEFAULT_ANNOVAR_URL,
    max_concurrent: int = 3,
    force: bool = False,
    **kwargs,
) -> Dict[str, Dict[str, Optional[Path]]]:
    """Download ANNOVAR files for multiple assemblies."""
    results = {}
    with start_action(
        action_type="download_annovar_files",
        assemblies=assemblies,
        output_folder=str(output_folder),
        files=files,
    ):
        for assembly in assemblies:
            downloader = AnnovarDownloader(
                assembly=assembly,
                base_url=base_url,
                output_folder=output_folder,
                max_concurrent=max_concurrent,
            )
            results[assembly] = downloader.download(files=files, force=force)
    return results

def download_annovar(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
    base_url: str = DEFAULT_ANNOVAR_URL,
    max_concurrent: int = 3,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """Legacy wrapper for downloading ANNOVAR files."""
    results_by_assembly = download_annovar_multiple_assemblies(
        assemblies=assemblies,
        output_folder=output_folder,
        files=files,
        base_url=base_url,
        max_concurrent=max_concurrent,
        force=force,
    )
    
    # Flatten results for backward compatibility
    flat_results = {}
    for assembly_results in results_by_assembly.values():
        flat_results.update(assembly_results)
        
    return flat_results
