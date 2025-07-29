"""
ClinVar database download and processing functions.
"""

import asyncio
import aiohttp
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
from eliot import start_action

from genobear.config import (
    DEFAULT_CLINVAR_URL, DEFAULT_CLINVAR_ASSEMBLIES_MAP,
    get_database_folder, get_parquet_path
)
from genobear.utils.download import download_file_async, create_default_symlink
from genobear.utils.conversion import convert_to_parquet


def construct_clinvar_url(
    base_url: str,
    assembly: str,
    assemblies_map: Dict[str, str],
    dated: bool = False,
    date_string: Optional[str] = None
) -> Optional[str]:
    """
    Construct the ClinVar download URL for a specific assembly.
    
    Args:
        base_url: Base URL for ClinVar downloads
        assembly: Genome assembly (e.g., 'hg38')
        assemblies_map: Mapping of assembly names to directory names
        dated: Whether to download dated version or latest
        date_string: Specific date string for dated version (e.g., '20250721')
        
    Returns:
        Complete URL for the ClinVar file, or None if assembly not supported
    """
    with start_action(action_type="construct_clinvar_url", 
                     base_url=base_url, assembly=assembly, dated=dated) as action:
        if assembly not in assemblies_map:
            action.log("Assembly not supported", supported_assemblies=list(assemblies_map.keys()))
            return None
        
        vcf_dir = assemblies_map[assembly]
        
        if dated and date_string:
            filename = f"clinvar_{date_string}.vcf.gz"
        else:
            filename = "clinvar.vcf.gz"
        
        url = urljoin(f"{base_url}/{vcf_dir}/", filename)
        
        action.add_success_fields(constructed_url=url, filename=filename, vcf_dir=vcf_dir)
        return url


async def download_clinvar(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    base_url: str = DEFAULT_CLINVAR_URL,
    assemblies_map: Optional[Dict[str, str]] = None,
    max_concurrent: int = 3,
    dated: bool = False,
    date_string: Optional[str] = None,
    force: bool = False,
    convert_to_parquet_files: bool = True,
    parquet_batch_size: int = 100_000
) -> Dict[str, Optional[Path]]:
    """
    Download ClinVar files for specified assemblies.
    
    Args:
        assemblies: List of genome assemblies to download
        output_folder: Base output folder for downloads (uses config default if None)
        base_url: Base URL for ClinVar downloads
        assemblies_map: Mapping of assembly names to directory names
        max_concurrent: Maximum concurrent downloads
        dated: Whether to download dated version or latest
        date_string: Specific date string for dated version
        force: Whether to force redownload/recreation of files
        convert_to_parquet_files: Whether to convert VCF files to Parquet
        parquet_batch_size: Batch size for Parquet conversion
        
    Returns:
        Dictionary mapping file paths to downloaded file paths (None if failed)
    """
    if output_folder is None:
        output_folder = get_database_folder("clinvar")
    
    if assemblies_map is None:
        assemblies_map = DEFAULT_CLINVAR_ASSEMBLIES_MAP
    
    with start_action(action_type="download_clinvar_files", 
                     assemblies=assemblies, 
                     output_folder=str(output_folder), 
                     max_concurrent=max_concurrent,
                     dated=dated) as action:
        
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        action.log("Starting ClinVar files download", assemblies_map=assemblies_map)
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            for assembly in assemblies:
                with start_action(action_type="prepare_clinvar_download", assembly=assembly) as prep_action:
                    # Construct URL
                    url = construct_clinvar_url(base_url, assembly, assemblies_map, dated, date_string)
                    if not url:
                        prep_action.log("Unsupported assembly, skipping")
                        continue
                    
                    # Construct destination path
                    dest_folder = output_folder / assembly
                    if dated and date_string:
                        dest_folder = dest_folder / date_string
                    else:
                        dest_folder = dest_folder / "latest"
                    
                    filename = Path(url).name
                    dest_path = dest_folder / filename
                    
                    # Skip if file already exists and not forcing
                    if dest_path.exists() and not force:
                        prep_action.log("File already exists, skipping", existing_file=str(dest_path))
                        results[str(dest_path)] = dest_path
                        continue
                    
                    prep_action.log("File queued for download", dest_path=str(dest_path))
                    
                    # Create download task with proper closure
                    def create_download_task(url_to_download, path_to_save):
                        async def download_with_semaphore():
                            async with semaphore:
                                return await download_file_async(session, url_to_download, path_to_save)
                        return download_with_semaphore()
                    
                    task = create_download_task(url, dest_path)
                    tasks.append((task, str(dest_path)))
                    
                    # Also download index file (.tbi)
                    tbi_url = f"{url}.tbi"
                    tbi_dest_path = dest_folder / f"{filename}.tbi"
                    if not tbi_dest_path.exists():
                        prep_action.log("Index file queued for download", index_file=str(tbi_dest_path))
                        tbi_task = create_download_task(tbi_url, tbi_dest_path)
                        tasks.append((tbi_task, str(tbi_dest_path)))
            
            action.log("Executing download tasks", total_tasks=len(tasks))
            
            # Execute all download tasks
            for task, file_path in tasks:
                result = await task
                results[file_path] = result
        
        # Convert to Parquet if requested
        if convert_to_parquet_files:
            conversion_results = await convert_clinvar_to_parquet(
                assemblies, output_folder, dated, date_string, force, parquet_batch_size
            )
            results.update(conversion_results)
        
        # Create default symlinks for latest downloads
        if not dated:
            for assembly in assemblies:
                assembly_folder = output_folder / assembly
                create_default_symlink("latest", assembly_folder)
        
        successful_downloads = sum(1 for result in results.values() if result is not None)
        failed_downloads = sum(1 for result in results.values() if result is None)
        
        action.add_success_fields(
            total_files=len(results),
            successful_downloads=successful_downloads,
            failed_downloads=failed_downloads,
            results=results
        )
        
        return results


async def convert_clinvar_to_parquet(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    dated: bool = False,
    date_string: Optional[str] = None,
    force: bool = False,
    batch_size: int = 100_000
) -> Dict[str, Optional[Path]]:
    """
    Convert ClinVar VCF files to Parquet format.
    
    Args:
        assemblies: List of genome assemblies
        output_folder: Base output folder (uses config default if None)
        dated: Whether files are dated versions
        date_string: Date string for dated versions
        force: Whether to force reconversion
        batch_size: Batch size for conversion
        
    Returns:
        Dictionary mapping parquet paths to results
    """
    if output_folder is None:
        output_folder = get_database_folder("clinvar")
    
    with start_action(action_type="convert_clinvar_to_parquet") as action:
        results = {}
        
        for assembly in assemblies:
            if dated and date_string:
                vcf_folder = output_folder / assembly / date_string
                release = date_string
            else:
                vcf_folder = output_folder / assembly / "latest"
                release = None
            
            vcf_files = list(vcf_folder.glob("*.vcf.gz"))
            
            action.log("Processing assembly", 
                     assembly=assembly, vcf_count=len(vcf_files))
            
            for vcf_file in vcf_files:
                # Use standardized parquet path
                parquet_file = get_parquet_path("clinvar", assembly, release)
                parquet_file.parent.mkdir(parents=True, exist_ok=True)
                
                if not parquet_file.exists() or force:
                    result = convert_to_parquet(
                        vcf_file, 
                        parquet_file, 
                        batch_size=batch_size
                    )
                    results[str(parquet_file)] = result
                else:
                    results[str(parquet_file)] = parquet_file
        
        return results