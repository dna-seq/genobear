"""
ANNOVAR database download functions.
"""

import asyncio
import aiohttp
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
from eliot import start_action

from genobear.config import DEFAULT_ANNOVAR_URL, get_database_folder
from genobear.utils.download import download_file_async


async def download_annovar(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
    base_url: str = DEFAULT_ANNOVAR_URL,
    max_concurrent: int = 3,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download ANNOVAR annotation database files for specified assemblies.
    
    Args:
        assemblies: List of genome assemblies to download
        output_folder: Base output folder for downloads (uses config default if None)
        files: List of specific file patterns to download, or None for minimum set
        base_url: Base URL for ANNOVAR downloads
        max_concurrent: Maximum concurrent downloads
        force: Whether to force redownload
        
    Returns:
        Dictionary mapping file paths to downloaded file paths (None if failed)
    """
    if output_folder is None:
        output_folder = get_database_folder("annovar")
    
    with start_action(action_type="download_annovar_files", 
                     assemblies=assemblies, 
                     output_folder=str(output_folder), 
                     max_concurrent=max_concurrent,
                     files=files) as action:
        
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        # Default minimum files if none specified
        files_minimum = ["refGene*"] if not files else files
        
        action.log("Starting ANNOVAR files download", files_patterns=files_minimum)
        
        async with aiohttp.ClientSession() as session:
            for assembly in assemblies:
                with start_action(action_type="download_annovar_assembly", assembly=assembly) as assembly_action:
                    # Create assembly folder
                    assembly_folder = output_folder / assembly
                    
                    # Download database list file first
                    avdblist_file = f"{assembly}_avdblist.txt"
                    avdblist_url = urljoin(f"{base_url}/", avdblist_file)
                    avdblist_path = assembly_folder / avdblist_file
                    
                    assembly_action.log("Downloading database list", 
                                      avdblist_url=avdblist_url,
                                      avdblist_path=str(avdblist_path))
                    
                    # Download avdblist file
                    async def download_with_semaphore(url, path):
                        async with semaphore:
                            return await download_file_async(session, url, path)
                    
                    avdblist_result = await download_with_semaphore(avdblist_url, avdblist_path)
                    if not avdblist_result:
                        assembly_action.log("Failed to download database list")
                        continue
                    
                    # Parse avdblist file to get available files
                    try:
                        available_files = []
                        with open(avdblist_path, 'r') as f:
                            for line in f:
                                parts = line.strip().split('\t')
                                if len(parts) >= 3:
                                    available_files.append({
                                        'file': parts[0],
                                        'version': parts[1], 
                                        'size': int(parts[2]) if parts[2].isdigit() else 0
                                    })
                        
                        assembly_action.log("Parsed database list", 
                                          available_count=len(available_files))
                        
                        # Determine files to download based on patterns
                        files_to_download = []
                        for pattern in files_minimum:
                            for available_file in available_files:
                                filename = available_file['file']
                                if pattern.replace('*', '') in filename and assembly in filename:
                                    files_to_download.append(available_file)
                        
                        assembly_action.log("Files matched patterns", 
                                          files_count=len(files_to_download))
                        
                        # Download each matched file
                        for file_info in files_to_download:
                            filename = file_info['file']
                            file_url = urljoin(f"{base_url}/", filename)
                            file_path = assembly_folder / filename
                            
                            # Skip if file already exists and has correct size (unless forcing)
                            if file_path.exists() and file_path.stat().st_size == file_info['size'] and not force:
                                assembly_action.log("File already exists with correct size", 
                                                  filename=filename)
                                results[str(file_path)] = file_path
                                continue
                            
                            # Download file
                            file_result = await download_with_semaphore(file_url, file_path)
                            results[str(file_path)] = file_result
                            
                            # Extract if it's a compressed file
                            if file_result and (filename.endswith('.gz') or filename.endswith('.zip')):
                                assembly_action.log("Extracting file", filename=filename)
                                try:
                                    if filename.endswith('.gz'):
                                        import gzip
                                        import shutil
                                        extracted_path = file_path.with_suffix('')
                                        with gzip.open(file_path, 'rb') as f_in:
                                            with open(extracted_path, 'wb') as f_out:
                                                shutil.copyfileobj(f_in, f_out)
                                    elif filename.endswith('.zip'):
                                        import zipfile
                                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                                            zip_ref.extractall(assembly_folder)
                                except Exception as e:
                                    assembly_action.log("Extraction failed", 
                                                      filename=filename, error=str(e))
                        
                    except Exception as e:
                        assembly_action.log("Failed to parse database list", error=str(e))
                        continue
        
        successful_downloads = sum(1 for result in results.values() if result is not None)
        failed_downloads = sum(1 for result in results.values() if result is None)
        
        action.add_success_fields(
            total_files=len(results),
            successful_downloads=successful_downloads,
            failed_downloads=failed_downloads,
            results=results
        )
        
        return results