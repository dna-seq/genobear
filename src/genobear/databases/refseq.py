"""
RefSeq database download and processing functions.
"""

import asyncio
import aiohttp
import biobear as bb
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
from eliot import start_action

from genobear.config import DEFAULT_REFSEQ_URL, get_database_folder
from genobear.utils.download import download_file_async


async def download_refseq(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
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
        output_folder: Base output folder for downloads (uses config default if None)
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
    if output_folder is None:
        output_folder = get_database_folder("refseq")
    
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