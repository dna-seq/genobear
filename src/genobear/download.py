#!/usr/bin/env python

import typer
from typing import List, Optional, Dict, Union
from pathlib import Path
import asyncio
import aiohttp
import aiofiles
from eliot import start_action
import sys
import os
from urllib.parse import urljoin
import biobear as bb

app = typer.Typer(help="Download genomic databases", no_args_is_help=True)

# Environment-configurable defaults
DEFAULT_DBSNP_URL = os.getenv("GENOBEAR_DBSNP_URL", "https://ftp.ncbi.nih.gov/snp/archive")
DEFAULT_DBSNP_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_DBSNP_FOLDER", "~/genobear/databases/dbsnp")))
DEFAULT_CLINVAR_URL = os.getenv("GENOBEAR_CLINVAR_URL", "https://ftp.ncbi.nlm.nih.gov/pub/clinvar")
DEFAULT_CLINVAR_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_CLINVAR_FOLDER", "~/genobear/databases/clinvar")))
DEFAULT_ANNOVAR_URL = os.getenv("GENOBEAR_ANNOVAR_URL", "http://www.openbioinformatics.org/annovar/download")
DEFAULT_ANNOVAR_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_ANNOVAR_FOLDER", "~/genobear/databases/annovar")))
DEFAULT_HGMD_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_HGMD_FOLDER", "~/genobear/databases/hgmd")))
DEFAULT_REFSEQ_URL = os.getenv("GENOBEAR_REFSEQ_URL", "https://hgdownload.soe.ucsc.edu/goldenPath")
DEFAULT_REFSEQ_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_REFSEQ_FOLDER", "~/genobear/databases/refseq")))
DEFAULT_EXOMISER_URL = os.getenv("GENOBEAR_EXOMISER_URL", "https://data.monarchinitiative.org/exomiser")
DEFAULT_EXOMISER_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_EXOMISER_FOLDER", "~/genobear/databases/exomiser")))
DEFAULT_ASSEMBLIES_MAP = {"hg19": "25", "hg38": "40"}
DEFAULT_CLINVAR_ASSEMBLIES_MAP = {"hg19": "vcf_GRCh37", "hg38": "vcf_GRCh38"}
DEFAULT_URL_PREFIX = os.getenv("GENOBEAR_DBSNP_URL_PREFIX", "GCF_000001405")
DEFAULT_ASSEMBLY = os.getenv("GENOBEAR_DEFAULT_ASSEMBLY", "hg38")
DEFAULT_RELEASE = os.getenv("GENOBEAR_DEFAULT_DBSNP_RELEASE", "b156")
DEFAULT_MAX_CONCURRENT = int(os.getenv("GENOBEAR_MAX_CONCURRENT_DOWNLOADS", "3"))
DEFAULT_PARQUET_BATCH_SIZE = int(os.getenv("GENOBEAR_PARQUET_BATCH_SIZE", "100000"))


async def download_file_async(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: Path,
    chunk_size: int = 8192
) -> Optional[Path]:
    """
    Download a file asynchronously with progress tracking.
    
    Args:
        session: aiohttp ClientSession for making requests
        url: URL to download from
        dest_path: Destination path for the downloaded file
        chunk_size: Size of chunks to download at a time
        
    Returns:
        Path to downloaded file if successful, None if failed
    """
    with start_action(action_type="download_file", url=url, dest_path=str(dest_path)) as action:
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            async with session.get(url) as response:
                if response.status == 200:
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded = 0
                    
                    action.log("Starting download", total_size=total_size)
                    
                    async with aiofiles.open(dest_path, 'wb') as file:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            await file.write(chunk)
                            downloaded += len(chunk)
                            
                            if total_size > 0:
                                progress = (downloaded / total_size) * 100
                                typer.echo(f"\rDownloading {dest_path.name}: {progress:.1f}%", nl=False)
                    
                    typer.echo(f"\n✅ Downloaded: {dest_path.name}")
                    action.add_success_fields(downloaded_bytes=downloaded, success=True)
                    return dest_path
                else:
                    action.log("HTTP error", status_code=response.status)
                    typer.echo(f"❌ Failed to download {url}: HTTP {response.status}")
                    return None
                    
        except Exception as e:
            action.log("Download failed with exception", error=str(e))
            typer.echo(f"❌ Error downloading {url}: {e}")
            return None


def construct_dbsnp_url(
    base_url: str,
    release: str,
    assembly: str,
    assemblies_map: Dict[str, str],
    url_prefix: str = DEFAULT_URL_PREFIX
) -> Optional[str]:
    """
    Construct the dbSNP download URL for a specific assembly and release.
    
    Args:
        base_url: Base URL for dbSNP downloads
        release: dbSNP release (e.g., 'b156')
        assembly: Genome assembly (e.g., 'hg38')
        assemblies_map: Mapping of assembly names to version numbers
        url_prefix: URL prefix for the file name
        
    Returns:
        Complete URL for the dbSNP file, or None if assembly not supported
    """
    with start_action(action_type="construct_dbsnp_url", 
                     base_url=base_url, release=release, assembly=assembly) as action:
        if assembly not in assemblies_map:
            action.log("Assembly not supported", supported_assemblies=list(assemblies_map.keys()))
            return None
        
        version = assemblies_map[assembly]
        filename = f"{url_prefix}.{version}.gz"
        url = urljoin(f"{base_url}/{release}/VCF/", filename)
        
        action.add_success_fields(constructed_url=url, filename=filename, version=version)
        return url


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


async def download_dbsnp_files(
    assemblies: List[str],
    releases: List[str],
    output_folder: Path,
    base_url: str = DEFAULT_DBSNP_URL,
    assemblies_map: Dict[str, str] = None,
    max_concurrent: int = 3,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download dbSNP files for specified assemblies and releases.
    
    Args:
        assemblies: List of genome assemblies to download
        releases: List of dbSNP releases to download  
        output_folder: Base output folder for downloads
        base_url: Base URL for dbSNP downloads
        assemblies_map: Mapping of assembly names to version numbers
        max_concurrent: Maximum concurrent downloads
        
    Returns:
        Dictionary mapping file paths to downloaded file paths (None if failed)
    """
    with start_action(action_type="download_dbsnp_files", 
                     assemblies=assemblies, releases=releases, 
                     output_folder=str(output_folder), max_concurrent=max_concurrent) as action:
        if assemblies_map is None:
            assemblies_map = DEFAULT_ASSEMBLIES_MAP
        
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        action.log("Starting dbSNP files download", assemblies_map=assemblies_map)
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            for assembly in assemblies:
                for release in releases:
                    with start_action(action_type="prepare_download", assembly=assembly, release=release) as prep_action:
                        # Construct URL
                        url = construct_dbsnp_url(base_url, release, assembly, assemblies_map)
                        if not url:
                            prep_action.log("Unsupported assembly, skipping")
                            continue
                        
                        # Construct destination path
                        dest_folder = output_folder / assembly / release
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
        
        successful_downloads = sum(1 for result in results.values() if result is not None)
        failed_downloads = sum(1 for result in results.values() if result is None)
        
        action.add_success_fields(
            total_files=len(results),
            successful_downloads=successful_downloads,
            failed_downloads=failed_downloads,
            results=results
        )
        
        return results


async def download_clinvar_files(
    assemblies: List[str],
    output_folder: Path,
    base_url: str = DEFAULT_CLINVAR_URL,
    assemblies_map: Dict[str, str] = None,
    max_concurrent: int = 3,
    dated: bool = False,
    date_string: Optional[str] = None,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download ClinVar files for specified assemblies.
    
    Args:
        assemblies: List of genome assemblies to download
        output_folder: Base output folder for downloads
        base_url: Base URL for ClinVar downloads
        assemblies_map: Mapping of assembly names to directory names
        max_concurrent: Maximum concurrent downloads
        dated: Whether to download dated version or latest
        date_string: Specific date string for dated version
        force: Whether to force redownload/recreation of files even if they already exist
        
    Returns:
        Dictionary mapping file paths to downloaded file paths (None if failed)
    """
    with start_action(action_type="download_clinvar_files", 
                     assemblies=assemblies, 
                     output_folder=str(output_folder), 
                     max_concurrent=max_concurrent,
                     dated=dated) as action:
        if assemblies_map is None:
            assemblies_map = DEFAULT_CLINVAR_ASSEMBLIES_MAP
        
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
        
        successful_downloads = sum(1 for result in results.values() if result is not None)
        failed_downloads = sum(1 for result in results.values() if result is None)
        
        action.add_success_fields(
            total_files=len(results),
            successful_downloads=successful_downloads,
            failed_downloads=failed_downloads,
            results=results
        )
        
        return results


async def download_annovar_files(
    assemblies: List[str],
    output_folder: Path,
    files: Optional[List[str]] = None,
    base_url: str = DEFAULT_ANNOVAR_URL,
    max_concurrent: int = 3,
    force: bool = False
) -> Dict[str, Optional[Path]]:
    """
    Download ANNOVAR annotation database files for specified assemblies.
    
    Args:
        assemblies: List of genome assemblies to download
        output_folder: Base output folder for downloads
        files: List of specific file patterns to download, or None for minimum set
        base_url: Base URL for ANNOVAR downloads
        max_concurrent: Maximum concurrent downloads
        
    Returns:
        Dictionary mapping file paths to downloaded file paths (None if failed)
    """
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


def convert_hgmd_to_formats(
    hgmd_vcf_path: Path,
    output_folder: Path,
    assembly: str,
    output_basename: Optional[str] = None,
    to_parquet: bool = True,
    to_tsv: bool = True,
    batch_size: int = 100_000
) -> Dict[str, Optional[Path]]:
    """
    Convert HGMD VCF file to Parquet and/or TSV formats using biobear.
    
    Args:
        hgmd_vcf_path: Path to input HGMD VCF file
        output_folder: Output folder for converted files
        assembly: Genome assembly (e.g., 'hg38')
        output_basename: Base name for output files
        to_parquet: Whether to generate Parquet files
        to_tsv: Whether to generate TSV files
        batch_size: Batch size for streaming conversion
        
    Returns:
        Dictionary mapping output format to file paths (None if failed)
    """
    with start_action(action_type="convert_hgmd_formats",
                     input_file=str(hgmd_vcf_path),
                     output_folder=str(output_folder),
                     assembly=assembly,
                     to_parquet=to_parquet,
                     to_tsv=to_tsv) as action:
        
        results = {}
        
        # Validate input file
        if not hgmd_vcf_path.exists():
            action.log("Input HGMD VCF file does not exist", reason="input_file_not_found")
            return results
        
        # Create output folder
        assembly_folder = output_folder / assembly
        assembly_folder.mkdir(parents=True, exist_ok=True)
        
        # Determine output basename
        if not output_basename:
            output_basename = hgmd_vcf_path.stem.replace(f"_{assembly}", "").replace(".vcf", "")
        
        action.log("Starting HGMD conversion", 
                  input_size=hgmd_vcf_path.stat().st_size,
                  output_basename=output_basename)
        
        # Create biobear session
        session = bb.connect()
        
        # Convert to Parquet if requested
        if to_parquet:
            parquet_path = assembly_folder / f"{output_basename}.parquet"
            if not parquet_path.exists():
                parquet_result = convert_to_parquet(hgmd_vcf_path, parquet_path, batch_size=batch_size)
                results['parquet'] = parquet_result
            else:
                action.log("Parquet file already exists", parquet_path=str(parquet_path))
                results['parquet'] = parquet_path
        
        # Convert to TSV if requested  
        if to_tsv:
            tsv_path = assembly_folder / f"{output_basename}.tsv"
            if not tsv_path.exists():
                with start_action(action_type="convert_hgmd_to_tsv") as tsv_action:
                    # Read VCF and convert to TSV using biobear streaming
                    reader = session.sql(f"""
                        SELECT * FROM read_vcf('{hgmd_vcf_path}')
                    """).to_arrow_record_batch_reader()
                    
                    # Write TSV with header
                    first_batch = True
                    rows_written = 0
                    
                    with open(tsv_path, 'w') as tsv_file:
                        for batch in reader:
                            if batch.num_rows > 0:
                                df = batch.to_pandas()
                                
                                # Write header only for first batch
                                df.to_csv(tsv_file, sep='\t', index=False, 
                                         header=first_batch, mode='a' if not first_batch else 'w')
                                first_batch = False
                                rows_written += len(df)
                    
                    tsv_action.add_success_fields(rows_written=rows_written)
                    results['tsv'] = tsv_path
            else:
                action.log("TSV file already exists", tsv_path=str(tsv_path))
                results['tsv'] = tsv_path
        
        action.add_success_fields(
            conversion_results=results,
            successful_conversions=sum(1 for r in results.values() if r is not None)
        )
            
        return results


async def download_refseq_files(
    assemblies: List[str],
    output_folder: Path,
    files: List[str] = None,
    base_url: str = DEFAULT_REFSEQ_URL,
    prefix: str = "ncbiRefSeq",
    max_concurrent: int = 3,
    format_to_bed: bool = True,
    include_utr_5: bool = True,
    include_utr_3: bool = True,
    include_chrm: bool = True,
    include_non_canonical_chr: bool = True,
    include_non_coding_transcripts: bool = True,
    include_transcript_ver: bool = True
) -> Dict[str, Dict[str, Optional[Path]]]:
    """
    Download RefSeq annotation files for specified assemblies.
    
    Args:
        assemblies: List of genome assemblies to download
        output_folder: Base output folder for downloads
        files: List of RefSeq files to download
        base_url: Base URL for RefSeq downloads
        prefix: Prefix for RefSeq files
        max_concurrent: Maximum concurrent downloads
        format_to_bed: Whether to format files to BED format
        include_utr_5: Include 5' UTR regions
        include_utr_3: Include 3' UTR regions
        include_chrm: Include mitochondrial chromosome
        include_non_canonical_chr: Include non-canonical chromosomes
        include_non_coding_transcripts: Include non-coding transcripts
        include_transcript_ver: Include transcript versions
        
    Returns:
        Dictionary mapping assemblies to downloaded files
    """
    with start_action(action_type="download_refseq_files",
                     assemblies=assemblies,
                     output_folder=str(output_folder),
                     max_concurrent=max_concurrent,
                     files=files) as action:
        
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        # Default files if none specified
        if not files:
            files = ["ncbiRefSeq.txt", "ncbiRefSeqLink.txt"]
            
        action.log("Starting RefSeq files download", 
                  files=files, format_to_bed=format_to_bed)
        
        async with aiohttp.ClientSession() as session:
            for assembly in assemblies:
                with start_action(action_type="download_refseq_assembly", assembly=assembly) as assembly_action:
                    results[assembly] = {}
                    assembly_folder = output_folder / assembly
                    
                    async def download_with_semaphore(url, path):
                        async with semaphore:
                            return await download_file_async(session, url, path)
                    
                    for file_name in files:
                        # Check if file already exists
                        local_file_path = assembly_folder / file_name
                        
                        if local_file_path.exists():
                            assembly_action.log("File already exists", filename=file_name)
                            results[assembly][file_name] = local_file_path
                            continue
                        
                        # Download compressed file
                        compressed_file = f"{file_name}.gz"
                        file_url = urljoin(f"{base_url}/{assembly}/database/", compressed_file)
                        compressed_path = assembly_folder / compressed_file
                        
                        assembly_action.log("Downloading RefSeq file",
                                          filename=file_name,
                                          url=file_url)
                        
                        compressed_result = await download_with_semaphore(file_url, compressed_path)
                        
                        if compressed_result:
                            # Extract the compressed file
                            try:
                                import gzip
                                with gzip.open(compressed_path, 'rb') as f_in:
                                    with open(local_file_path, 'wb') as f_out:
                                        f_out.write(f_in.read())
                                
                                assembly_action.log("File extracted successfully", filename=file_name)
                                results[assembly][file_name] = local_file_path
                                
                                # Format to BED if requested and this is the main RefSeq file
                                if format_to_bed and file_name == "ncbiRefSeq.txt":
                                    bed_path = local_file_path.with_suffix('.bed')
                                    if not bed_path.exists():
                                        try:
                                            format_refseq_to_bed(
                                                local_file_path, 
                                                bed_path,
                                                include_utr_5=include_utr_5,
                                                include_utr_3=include_utr_3,
                                                include_chrm=include_chrm,
                                                include_non_canonical_chr=include_non_canonical_chr,
                                                include_non_coding_transcripts=include_non_coding_transcripts,
                                                include_transcript_ver=include_transcript_ver
                                            )
                                            assembly_action.log("RefSeq formatted to BED", bed_file=str(bed_path))
                                        except Exception as e:
                                            assembly_action.log("BED formatting failed", error=str(e))
                                
                            except Exception as e:
                                assembly_action.log("Extraction failed", 
                                                  filename=file_name, error=str(e))
                                results[assembly][file_name] = None
                        else:
                            assembly_action.log("Download failed", filename=file_name)
                            results[assembly][file_name] = None
        
        total_files = sum(len(assembly_files) for assembly_files in results.values())
        successful_downloads = sum(
            1 for assembly_files in results.values() 
            for result in assembly_files.values() 
            if result is not None
        )
        
        action.add_success_fields(
            total_files=total_files,
            successful_downloads=successful_downloads,
            results=results
        )
        
        return results


def format_refseq_to_bed(
    refseq_file: Path,
    output_file: Path,
    include_utr_5: bool = True,
    include_utr_3: bool = True,
    include_chrm: bool = True,
    include_non_canonical_chr: bool = True,
    include_non_coding_transcripts: bool = True,
    include_transcript_ver: bool = True
) -> None:
    """
    Format RefSeq file to BED format using biobear for efficient processing.
    
    Args:
        refseq_file: Input RefSeq file path
        output_file: Output BED file path
        include_utr_5: Include 5' UTR regions
        include_utr_3: Include 3' UTR regions  
        include_chrm: Include mitochondrial chromosome
        include_non_canonical_chr: Include non-canonical chromosomes
        include_non_coding_transcripts: Include non-coding transcripts
        include_transcript_ver: Include transcript versions
    """
    with start_action(action_type="format_refseq_to_bed",
                     input_file=str(refseq_file),
                     output_file=str(output_file)) as action:
        # Create biobear session
        session = bb.connect()
        
        # Build filter conditions
        conditions = []
        
        if not include_chrm:
            conditions.append("chrom != 'chrM'")
        
        if not include_non_canonical_chr:
            conditions.append("chrom RLIKE '^chr[0-9XY]+$'")
        
        # Create filter clause
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Process RefSeq file with SQL
        query = f"""
            SELECT 
                chrom,
                CAST(txStart AS INTEGER) as start,
                CAST(txEnd AS INTEGER) as end,
                CASE 
                    WHEN {include_transcript_ver} THEN name
                    ELSE SPLIT_PART(name, '.', 1)
                END as name,
                0 as score,
                strand,
                CAST(cdsStart AS INTEGER) as thickStart,
                CAST(cdsEnd AS INTEGER) as thickEnd,
                '0,0,0' as itemRgb,
                CAST(exonCount AS INTEGER) as blockCount,
                exonStarts,
                exonEnds
            FROM read_csv('{refseq_file}', 
                         delimiter='\t', 
                         header=false,
                         column_names=['bin', 'name', 'chrom', 'strand', 'txStart', 'txEnd', 
                                     'cdsStart', 'cdsEnd', 'exonCount', 'exonStarts', 'exonEnds', 
                                     'score', 'name2', 'cdsStartStat', 'cdsEndStat', 'exonFrames'])
            WHERE {where_clause}
            ORDER BY chrom, start
        """
        
        # Execute query and write to BED file
        result = session.sql(query)
        df = result.to_pandas()
        
        # Write BED file with proper formatting
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, sep='\t', index=False, header=False)
        
        action.add_success_fields(
            records_processed=len(df),
            output_size=output_file.stat().st_size if output_file.exists() else 0
        )


async def download_exomiser_files(
    assemblies: List[str],
    output_folder: Path,
    base_url: str = DEFAULT_EXOMISER_URL,
    exomiser_release: Optional[str] = None,
    phenotype_release: Optional[str] = None,
    max_concurrent: int = 3
) -> Dict[str, Dict[str, Optional[Path]]]:
    """
    Download Exomiser database files for specified assemblies.
    
    Args:
        assemblies: List of genome assemblies to download
        output_folder: Base output folder for downloads
        base_url: Base URL for Exomiser downloads
        exomiser_release: Specific Exomiser release version
        phenotype_release: Specific phenotype release version
        max_concurrent: Maximum concurrent downloads
        
    Returns:
        Dictionary mapping assemblies to downloaded files
    """
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


def convert_to_parquet(
    vcf_path: Path,
    output_path: Path,
    explode_infos: bool = True,
    batch_size: int = 100_000
) -> Optional[Path]:
    """
    Convert VCF file to Parquet format using biobear with streaming processing.
    
    Args:
        vcf_path: Path to input VCF file
        output_path: Path for output Parquet file
        explode_infos: Whether to explode INFO fields into separate columns
        batch_size: Number of rows to process in each batch
        
    Returns:
        Path to created Parquet file if successful, None if failed
    """
    with start_action(action_type="convert_vcf_to_parquet", 
                     input_file=str(vcf_path), 
                     output_file=str(output_path),
                     explode_infos=explode_infos,
                     batch_size=batch_size) as action:
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        action.log("Starting streaming VCF to Parquet conversion")
        
        # Create biobear session
        session = bb.connect()
        
        # Read VCF file as streaming Arrow record batch reader
        action.log("Setting up streaming VCF reader with biobear")
        reader = session.sql(f"""
            SELECT * FROM read_vcf('{vcf_path}')
        """).to_arrow_record_batch_reader()
        
        # Create output directory
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Stream process batches and write to Parquet
        action.log("Starting streaming conversion to Parquet")
        
        # Get schema from first batch to set up Parquet writer
        first_batch = None
        total_rows = 0
        batches_processed = 0
        
        # Process the stream in batches
        with pq.ParquetWriter(str(output_path), reader.schema) as writer:
            for batch in reader:
                if batch.num_rows > 0:
                    writer.write_batch(batch)
                    total_rows += batch.num_rows
                    batches_processed += 1
                    
                    if batches_processed % 10 == 0:  # Log every 10 batches
                        action.log(
                            "Processing batches", 
                            batches_processed=batches_processed,
                            rows_processed=total_rows
                        )
        
        action.log("Completed streaming conversion", 
                  total_batches=batches_processed,
                  total_rows=total_rows)
        
        action.add_success_fields(
            input_size=vcf_path.stat().st_size,
            output_size=output_path.stat().st_size,
            rows_converted=total_rows,
            batches_processed=batches_processed,
            success=True
        )
        return output_path


def create_default_symlink(
    source_release: str,
    assembly_folder: Path
) -> Optional[Path]:
    """
    Create a symlink to mark a release as default.
    
    Args:
        source_release: Name of the release to link to
        assembly_folder: Folder containing the assembly releases
        
    Returns:
        Path to created symlink if successful, None if failed
    """
    with start_action(action_type="create_default_symlink", 
                     source_release=source_release, 
                     assembly_folder=str(assembly_folder)) as action:
        default_link = assembly_folder / "default"
        source_path = assembly_folder / source_release
        
        if not source_path.exists():
            action.log("Source release folder does not exist", source_path=str(source_path), reason="source_not_found")
            return None
        
        # Remove existing symlink if it exists
        if default_link.is_symlink():
            action.log("Removing existing symlink")
            default_link.unlink()
        elif default_link.exists():
            action.log("Default path exists but is not a symlink", path=str(default_link), reason="path_not_symlink")
            return None
        
        # Create new symlink
        default_link.symlink_to(source_release)
        action.add_success_fields(
            symlink_created=str(default_link),
            target=source_release,
            success=True
        )
        return default_link


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
    default_release: Optional[str] = typer.Option(
        None,
        "--default-release",
        help="Default release to link. If not specified, uses first release."
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
    base_url: str = typer.Option(
        DEFAULT_DBSNP_URL,
        "--base-url",
        help="Base URL for dbSNP downloads"
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
        python -m genobear download dbsnp
        python -m genobear download dbsnp hg19 hg38 --release b156 --parquet
        python -m genobear download dbsnp hg38 --output /path/to/dbsnp --max-concurrent 5 --parquet-batch-size 50000
    """
    with start_action(action_type="download_dbsnp_command",
                     assemblies=assemblies,
                     releases=releases,
                     max_concurrent=max_concurrent,
                     generate_parquet=generate_parquet,
                     parquet_batch_size=parquet_batch_size) as action:
        try:
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = DEFAULT_DBSNP_FOLDER
                action.log("Using default output folder from config", folder=str(output_folder))
            
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
                      output_folder=str(output_folder),
                      base_url=base_url)
            
            # Download files
            results = asyncio.run(download_dbsnp_files(
                assemblies=assemblies,
                releases=releases,
                output_folder=output_folder,
                base_url=base_url,
                max_concurrent=max_concurrent,
                force=force
            ))
            
            # Check results
            failed_downloads = [path for path, result in results.items() if result is None]
            if failed_downloads:
                action.log("Some downloads failed", failed_count=len(failed_downloads))
                typer.echo(f"❌ Failed downloads: {len(failed_downloads)}")
                for path in failed_downloads:
                    typer.echo(f"  - {path}")

                raise typer.Exit(1)
            
            # Generate Parquet files if requested
            if generate_parquet:
                with start_action(action_type="convert_all_to_parquet") as parquet_action:
                    typer.echo("Converting VCF files to Parquet...")
                    conversion_results = []
                    
                    for assembly in assemblies:
                        for release in releases:
                            vcf_folder = output_folder / assembly / release
                            vcf_files = list(vcf_folder.glob("*.vcf.gz"))
                            
                            parquet_action.log("Processing assembly/release", 
                                             assembly=assembly, release=release, vcf_count=len(vcf_files))
                            
                            for vcf_file in vcf_files:
                                parquet_file = vcf_file.with_suffix('.parquet')
                                if not parquet_file.exists() or force:
                                    result = convert_to_parquet(
                                        vcf_file, 
                                        parquet_file, 
                                        batch_size=parquet_batch_size
                                    )
                                    conversion_results.append(result is not None)
                    
                    parquet_action.add_success_fields(
                        total_conversions=len(conversion_results),
                        successful_conversions=sum(conversion_results)
                    )
            
            # Create default symlinks
            default_release_to_use = default_release or releases[0]
            action.log("Creating default symlinks", default_release=default_release_to_use)
            
            for assembly in assemblies:
                assembly_folder = output_folder / assembly
                create_default_symlink(default_release_to_use, assembly_folder)
            
            successful_downloads = len([r for r in results.values() if r is not None])
            action.add_success_fields(
                successful_downloads=successful_downloads,
                total_files=len(results),
                assemblies_processed=assemblies,
                releases_processed=releases
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_downloads} files for {', '.join(assemblies)}",
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
    base_url: str = typer.Option(
        DEFAULT_CLINVAR_URL,
        "--base-url",
        help="Base URL for ClinVar downloads"
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
        python -m genobear download clinvar
        python -m genobear download clinvar hg19 hg38 --parquet
        python -m genobear download clinvar hg38 --dated --date 20250721 --parquet
        python -m genobear download clinvar --output /data/clinvar --max-concurrent 5
    """
    with start_action(action_type="download_clinvar_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     generate_parquet=generate_parquet,
                     dated=dated,
                     date_string=date_string) as action:
        try:
            # Validate date requirement for dated downloads
            if dated and not date_string:
                action.log("Date string required for dated downloads")
                typer.echo("❌ --date is required when using --dated")

                raise typer.Exit(1)
            
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = DEFAULT_CLINVAR_FOLDER
                action.log("Using default output folder from config", folder=str(output_folder))
            
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
                      output_folder=str(output_folder),
                      base_url=base_url)
            
            # Download files
            results = asyncio.run(download_clinvar_files(
                assemblies=assemblies,
                output_folder=output_folder,
                base_url=base_url,
                max_concurrent=max_concurrent,
                dated=dated,
                date_string=date_string,
                force=force
            ))
            
            # Check results
            failed_downloads = [path for path, result in results.items() if result is None]
            if failed_downloads:
                action.log("Some downloads failed", failed_count=len(failed_downloads))
                typer.echo(f"❌ Failed downloads: {len(failed_downloads)}")
                for path in failed_downloads:
                    typer.echo(f"  - {path}")

                raise typer.Exit(1)
            
            # Generate Parquet files if requested
            if generate_parquet:
                with start_action(action_type="convert_all_clinvar_to_parquet") as parquet_action:
                    typer.echo("Converting ClinVar VCF files to Parquet...")
                    conversion_results = []
                    
                    for assembly in assemblies:
                        if dated and date_string:
                            vcf_folder = output_folder / assembly / date_string
                        else:
                            vcf_folder = output_folder / assembly / "latest"
                        
                        vcf_files = list(vcf_folder.glob("*.vcf.gz"))
                        
                        parquet_action.log("Processing assembly", 
                                         assembly=assembly, vcf_count=len(vcf_files))
                        
                        for vcf_file in vcf_files:
                            parquet_file = vcf_file.with_suffix('.parquet')
                            if not parquet_file.exists() or force:
                                result = convert_to_parquet(
                                    vcf_file, 
                                    parquet_file, 
                                    batch_size=parquet_batch_size
                                )
                                conversion_results.append(result is not None)
                    
                    parquet_action.add_success_fields(
                        total_conversions=len(conversion_results),
                        successful_conversions=sum(conversion_results)
                    )
            
            # Create default symlinks for latest downloads
            if not dated:
                for assembly in assemblies:
                    assembly_folder = output_folder / assembly
                    create_default_symlink("latest", assembly_folder)
            
            successful_downloads = len([r for r in results.values() if r is not None])
            action.add_success_fields(
                successful_downloads=successful_downloads,
                total_files=len(results),
                assemblies_processed=assemblies
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_downloads} ClinVar files for {', '.join(assemblies)}",
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
    base_url: str = typer.Option(
        DEFAULT_ANNOVAR_URL,
        "--base-url",
        help="Base URL for ANNOVAR downloads"
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
        python -m genobear download annovar hg38
        python -m genobear download annovar hg19 hg38 --files refGene clinvar_20230416
        python -m genobear download annovar hg38 --output /data/annovar --max-concurrent 5
    """
    with start_action(action_type="download_annovar_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     files=files) as action:
        try:
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = DEFAULT_ANNOVAR_FOLDER
                action.log("Using default output folder from config", folder=str(output_folder))
            
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
                      output_folder=str(output_folder),
                      base_url=base_url)
            
            # Download files
            results = asyncio.run(download_annovar_files(
                assemblies=assemblies,
                output_folder=output_folder,
                files=files,
                base_url=base_url,
                max_concurrent=max_concurrent,
                force=force
            ))
            
            # Check results
            failed_downloads = [path for path, result in results.items() if result is None]
            if failed_downloads:
                action.log("Some downloads failed", failed_count=len(failed_downloads))
                typer.echo(f"❌ Failed downloads: {len(failed_downloads)}")
                for path in failed_downloads:
                    typer.echo(f"  - {path}")

                raise typer.Exit(1)
            
            successful_downloads = len([r for r in results.values() if r is not None])
            action.add_success_fields(
                successful_downloads=successful_downloads,
                total_files=len(results),
                assemblies_processed=assemblies
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_downloads} ANNOVAR files for {', '.join(assemblies)}",
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
        python -m genobear download hgmd /path/to/hgmd.vcf.gz hg38
        python -m genobear download hgmd /path/to/hgmd.vcf.gz hg38 --no-tsv --basename hgmd_2024
        python -m genobear download hgmd /path/to/hgmd.vcf.gz hg38 --output /data/hgmd
    """
    with start_action(action_type="convert_hgmd_command",
                     hgmd_file=str(hgmd_file),
                     assembly=assembly,
                     to_parquet=to_parquet,
                     to_tsv=to_tsv) as action:
        try:
            # Validate input file
            if not hgmd_file.exists():
                action.log("Input HGMD file does not exist")
                typer.echo(f"❌ Input file does not exist: {hgmd_file}")

                raise typer.Exit(1)
            
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = DEFAULT_HGMD_FOLDER
                action.log("Using default output folder from config", folder=str(output_folder))
            
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
            
            # Convert files
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
    include_utr_5: bool = typer.Option(
        True,
        "--include-utr5/--no-utr5",
        help="Include 5' UTR regions in BED formatting"
    ),
    include_utr_3: bool = typer.Option(
        True,
        "--include-utr3/--no-utr3",
        help="Include 3' UTR regions in BED formatting"
    ),
    include_chrm: bool = typer.Option(
        True,
        "--include-chrm/--no-chrm",
        help="Include mitochondrial chromosome"
    ),
    include_non_canonical_chr: bool = typer.Option(
        True,
        "--include-non-canonical/--no-non-canonical",
        help="Include non-canonical chromosomes"
    ),
    include_non_coding_transcripts: bool = typer.Option(
        True,
        "--include-non-coding/--no-non-coding",
        help="Include non-coding transcripts"
    ),
    include_transcript_ver: bool = typer.Option(
        True,
        "--include-version/--no-version",
        help="Include transcript versions"
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT,
        "--max-concurrent",
        help=f"Maximum concurrent downloads. Default: {DEFAULT_MAX_CONCURRENT}"
    ),
    base_url: str = typer.Option(
        DEFAULT_REFSEQ_URL,
        "--base-url",
        help="Base URL for RefSeq downloads"
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
        python -m genobear download refseq hg38
        python -m genobear download refseq hg19 hg38 --no-format-bed
        python -m genobear download refseq hg38 --files ncbiRefSeq.txt ncbiRefSeqLink.txt --output /data/refseq
    """
    with start_action(action_type="download_refseq_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     files=files,
                     format_to_bed=format_to_bed) as action:
        try:
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = DEFAULT_REFSEQ_FOLDER
                action.log("Using default output folder from config", folder=str(output_folder))
            
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
                      output_folder=str(output_folder),
                      base_url=base_url)
            
            # Download files
            results = asyncio.run(download_refseq_files(
                assemblies=assemblies,
                output_folder=output_folder,
                files=files,
                base_url=base_url,
                max_concurrent=max_concurrent,
                format_to_bed=format_to_bed,
                include_utr_5=include_utr_5,
                include_utr_3=include_utr_3,
                include_chrm=include_chrm,
                include_non_canonical_chr=include_non_canonical_chr,
                include_non_coding_transcripts=include_non_coding_transcripts,
                include_transcript_ver=include_transcript_ver
            ))
            
            # Check results
            failed_downloads = []
            successful_downloads = 0
            for assembly, assembly_files in results.items():
                for file_name, result in assembly_files.items():
                    if result is None:
                        failed_downloads.append(f"{assembly}/{file_name}")
                    else:
                        successful_downloads += 1
            
            if failed_downloads:
                action.log("Some downloads failed", failed_count=len(failed_downloads))
                typer.echo(f"❌ Failed downloads: {len(failed_downloads)}")
                for path in failed_downloads:
                    typer.echo(f"  - {path}")

                if successful_downloads == 0:
                    raise typer.Exit(1)
            
            action.add_success_fields(
                successful_downloads=successful_downloads,
                total_assemblies=len(assemblies),
                assemblies_processed=assemblies
            )
            
            typer.echo(
                typer.style(
                    f"✅ Successfully downloaded {successful_downloads} RefSeq files for {', '.join(assemblies)}",
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
    base_url: str = typer.Option(
        DEFAULT_EXOMISER_URL,
        "--base-url",
        help="Base URL for Exomiser downloads"
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
        python -m genobear download exomiser hg38
        python -m genobear download exomiser hg19 hg38 --exomiser-release 2402
        python -m genobear download exomiser hg38 --output /data/exomiser --max-concurrent 5
    """
    with start_action(action_type="download_exomiser_command",
                     assemblies=assemblies,
                     max_concurrent=max_concurrent,
                     exomiser_release=exomiser_release,
                     phenotype_release=phenotype_release) as action:
        try:
            # Set default output folder if not provided
            if output_folder is None:
                output_folder = DEFAULT_EXOMISER_FOLDER
                action.log("Using default output folder from config", folder=str(output_folder))
            
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
                      output_folder=str(output_folder),
                      base_url=base_url)
            
            # Download files
            results = asyncio.run(download_exomiser_files(
                assemblies=assemblies,
                output_folder=output_folder,
                base_url=base_url,
                exomiser_release=exomiser_release,
                phenotype_release=phenotype_release,
                max_concurrent=max_concurrent
            ))
            
            # Check results
            failed_assemblies = []
            successful_assemblies = 0
            for assembly, assembly_results in results.items():
                if not any(result is not None for result in assembly_results.values()):
                    failed_assemblies.append(assembly)
                else:
                    successful_assemblies += 1
            
            if failed_assemblies:
                action.log("Some assembly downloads failed", failed_assemblies=failed_assemblies)
                typer.echo(f"❌ Failed assemblies: {', '.join(failed_assemblies)}")

                if successful_assemblies == 0:
                    raise typer.Exit(1)
            
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
        
        # Log database count for monitoring
        
        typer.echo("Available databases:")
        for name, description, versions in databases:
            typer.echo(f"\n  🧬 {name}")
            typer.echo(f"     {description}")
            typer.echo(f"     {versions}")
        
        typer.echo(f"\nUsage examples:")
        typer.echo(f"  python -m genobear download dbsnp hg38 --parquet")
        typer.echo(f"  python -m genobear download clinvar hg38 --parquet")
        typer.echo(f"  python -m genobear download clinvar hg38 --dated --date 20250721")
        typer.echo(f"  python -m genobear download annovar hg38 --files refGene clinvar")
        typer.echo(f"  python -m genobear download hgmd /path/to/hgmd.vcf.gz hg38")
        typer.echo(f"  python -m genobear download refseq hg38 --format-bed")
        typer.echo(f"  python -m genobear download exomiser hg38 --exomiser-release 2402")
        
        action.add_success_fields(databases_count=len(databases))


if __name__ == "__main__":
    app() 