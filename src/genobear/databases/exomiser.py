"""
Exomiser database download functions.
"""

import asyncio
import aiohttp
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
from eliot import start_action

from genobear.config import DEFAULT_EXOMISER_URL, get_database_folder
from genobear.utils.download import download_file_async


async def download_exomiser(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    base_url: str = DEFAULT_EXOMISER_URL,
    exomiser_release: Optional[str] = None,
    phenotype_release: Optional[str] = None,
    max_concurrent: int = 3
) -> Dict[str, Dict[str, Optional[Path]]]:
    """
    Download Exomiser database files for specified assemblies.
    
    Args:
        assemblies: List of genome assemblies to download
        output_folder: Base output folder for downloads (uses config default if None)
        base_url: Base URL for Exomiser downloads
        exomiser_release: Specific Exomiser release version
        phenotype_release: Specific phenotype release version
        max_concurrent: Maximum concurrent downloads
        
    Returns:
        Dictionary mapping assemblies to downloaded files
    """
    if output_folder is None:
        output_folder = get_database_folder("exomiser")
    
    with start_action(action_type="download_exomiser_files",
                     assemblies=assemblies,
                     output_folder=str(output_folder),
                     max_concurrent=max_concurrent,
                     exomiser_release=exomiser_release,
                     phenotype_release=phenotype_release) as action:
        
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        # Default releases
        default_exomiser_release = exomiser_release or "2402"
        default_phenotype_release = phenotype_release or default_exomiser_release
        
        action.log("Starting Exomiser files download",
                  exomiser_release=default_exomiser_release,
                  phenotype_release=default_phenotype_release)
        
        async with aiohttp.ClientSession() as session:
            # Download phenotype data (shared across assemblies)
            phenotype_filename = f"{default_phenotype_release}_phenotype.zip"
            phenotype_url = urljoin(f"{base_url}/data/", phenotype_filename)
            phenotype_path = output_folder / phenotype_filename
            
            async def download_with_semaphore(url, path):
                async with semaphore:
                    return await download_file_async(session, url, path)
            
            # Download phenotype file if not exists
            if not phenotype_path.exists():
                action.log("Downloading phenotype data", url=phenotype_url)
                phenotype_result = await download_with_semaphore(phenotype_url, phenotype_path)
                
                # Extract phenotype data
                if phenotype_result:
                    phenotype_folder = output_folder / f"{default_phenotype_release}_phenotype"
                    if not phenotype_folder.exists():
                        try:
                            import zipfile
                            with zipfile.ZipFile(phenotype_path, 'r') as zip_ref:
                                zip_ref.extractall(output_folder)
                            action.log("Phenotype data extracted")
                        except Exception as e:
                            action.log("Phenotype extraction failed", error=str(e))
            else:
                action.log("Phenotype data already exists")
            
            # Download assembly-specific data
            for assembly in assemblies:
                with start_action(action_type="download_exomiser_assembly", assembly=assembly) as assembly_action:
                    results[assembly] = {}
                    assembly_folder = output_folder / assembly
                    
                    # Convert assembly names for Exomiser (hg19 -> GRCh37, hg38 -> GRCh38)
                    exomiser_assembly = assembly
                    if assembly == "hg19":
                        exomiser_assembly = "GRCh37"
                    elif assembly == "hg38":
                        exomiser_assembly = "GRCh38"
                    
                    # Download assembly data
                    assembly_filename = f"{default_exomiser_release}_{exomiser_assembly}.zip"
                    assembly_url = urljoin(f"{base_url}/data/", assembly_filename)
                    assembly_path = output_folder / assembly_filename
                    
                    if not assembly_path.exists():
                        assembly_action.log("Downloading assembly data", 
                                          url=assembly_url, filename=assembly_filename)
                        assembly_result = await download_with_semaphore(assembly_url, assembly_path)
                        
                        # Extract assembly data
                        if assembly_result:
                            assembly_data_folder = output_folder / f"{default_exomiser_release}_{exomiser_assembly}"
                            if not assembly_data_folder.exists():
                                try:
                                    import zipfile
                                    with zipfile.ZipFile(assembly_path, 'r') as zip_ref:
                                        zip_ref.extractall(output_folder)
                                    assembly_action.log("Assembly data extracted")
                                    results[assembly]['data'] = assembly_data_folder
                                except Exception as e:
                                    assembly_action.log("Assembly extraction failed", error=str(e))
                                    results[assembly]['data'] = None
                            else:
                                assembly_action.log("Assembly data already extracted")
                                results[assembly]['data'] = assembly_data_folder
                        else:
                            assembly_action.log("Assembly download failed")
                            results[assembly]['data'] = None
                    else:
                        assembly_action.log("Assembly data already exists")
                        results[assembly]['data'] = assembly_path
                    
                    # Create application.properties file
                    try:
                        create_exomiser_properties(
                            assembly_folder, 
                            assembly, 
                            default_exomiser_release, 
                            default_phenotype_release,
                            output_folder
                        )
                        results[assembly]['properties'] = assembly_folder / "application.properties"
                        assembly_action.log("Application properties created")
                    except Exception as e:
                        assembly_action.log("Properties creation failed", error=str(e))
                        results[assembly]['properties'] = None
        
        total_assemblies = len(assemblies)
        successful_assemblies = sum(
            1 for assembly_results in results.values()
            if any(result is not None for result in assembly_results.values())
        )
        
        action.add_success_fields(
            total_assemblies=total_assemblies,
            successful_assemblies=successful_assemblies,
            results=results
        )
        
        return results


def create_exomiser_properties(
    assembly_folder: Path,
    assembly: str,
    exomiser_release: str,
    phenotype_release: str,
    data_folder: Path
) -> None:
    """
    Create Exomiser application.properties file for an assembly.
    
    Args:
        assembly_folder: Assembly-specific folder
        assembly: Genome assembly name
        exomiser_release: Exomiser release version
        phenotype_release: Phenotype release version
        data_folder: Base data folder containing Exomiser files
    """
    with start_action(action_type="create_exomiser_properties",
                     assembly=assembly,
                     assembly_folder=str(assembly_folder)) as action:
        
        assembly_folder.mkdir(parents=True, exist_ok=True)
        properties_file = assembly_folder / "application.properties"
        
        # Convert assembly names for Exomiser
        exomiser_assembly = assembly
        if assembly == "hg19":
            exomiser_assembly = "GRCh37"
        elif assembly == "hg38":
            exomiser_assembly = "GRCh38"
        
        properties_content = f"""# Exomiser application properties for {assembly}
# Generated by GenoBear

# Data directory
exomiser.data-directory={data_folder}

# Data version
exomiser.{assembly}.data-version={exomiser_release}

# Phenotype data version  
exomiser.phenotype.data-version={phenotype_release}

# Transcript source
exomiser.{assembly}.transcript-source=refseq

# Variant white list path (if available)
exomiser.{assembly}.variant-white-list-path={exomiser_release}_{assembly}_clinvar_whitelist.tsv.gz

# ReMM path
exomiser.{assembly}.remm-path=${{exomiser.data-directory}}/{exomiser_release}_{exomiser_assembly}_remm.tsv.gz

# CADD SNV path
exomiser.{assembly}.cadd-snv-path=${{exomiser.data-directory}}/{exomiser_release}_{exomiser_assembly}_cadd_snv.tsv.gz

# CADD InDel path  
exomiser.{assembly}.cadd-indel-path=${{exomiser.data-directory}}/{exomiser_release}_{exomiser_assembly}_cadd_indel.tsv.gz
"""
        
        with open(properties_file, 'w') as f:
            f.write(properties_content)
        
        action.log("Properties file created", file=str(properties_file))