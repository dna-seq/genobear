"""
RefSeq database download and processing functions.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import biobear as bb
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
from eliot import start_action

from genobear.config import DEFAULT_REFSEQ_URL, get_database_folder
from genobear.databases.base import ArchiveDatabaseDownloader



class RefSeqDownloader(ArchiveDatabaseDownloader):
    """RefSeq database downloader."""

    @property
    def database_name(self) -> str:
        return "refseq"

    def download(
        self,
        files: Optional[List[str]] = None,
        force: bool = False,
        format_to_bed: bool = True,
        **kwargs,
    ) -> Dict[str, Optional[Path]]:
        """Download RefSeq files for a specific assembly."""
        files = files or ["ncbiRefSeq.txt", "ncbiRefSeqLink.txt"]
        
        with start_action(
            action_type="download_refseq_assembly",
            assembly=self.assembly,
            files=files,
        ) as action:
            results: Dict[str, Optional[Path]] = {}
            assembly_folder = self.assembly_folder
            assembly_folder.mkdir(parents=True, exist_ok=True)

            download_tasks = []
            for file_name in files:
                local_path = assembly_folder / file_name
                if not local_path.exists() or force:
                    url = urljoin(
                        f"{self.base_url}/{self.assembly}/database/", f"{file_name}.gz"
                    )
                    compressed_path = assembly_folder / f"{file_name}.gz"
                    download_tasks.append((url, compressed_path, local_path, file_name))

            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                future_to_file = {
                    executor.submit(self.download_file, url, cpath): (lpath, fname)
                    for url, cpath, lpath, fname in download_tasks
                }

                for future in as_completed(future_to_file):
                    local_path, file_name = future_to_file[future]
                    if future.result():
                        self.extract_archive(local_path.with_suffix(".gz"), assembly_folder)
                        results[file_name] = local_path
                        if format_to_bed and "ncbiRefSeq.txt" in file_name:
                            self.format_to_bed(local_path, **kwargs)

            return results

    def format_to_bed(self, refseq_file: Path, **kwargs) -> None:
        """Format the RefSeq file to BED format."""
        output_file = refseq_file.with_suffix(".bed")
        if not output_file.exists():
            format_refseq_to_bed(refseq_file, output_file, **kwargs)


def download_refseq_multiple_assemblies(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
    base_url: str = DEFAULT_REFSEQ_URL,
    max_concurrent: int = 3,
    force: bool = False,
    **kwargs,
) -> Dict[str, Dict[str, Optional[Path]]]:
    """Download RefSeq files for multiple assemblies."""
    results = {}
    with start_action(
        action_type="download_refseq_files",
        assemblies=assemblies,
        output_folder=str(output_folder),
    ):
        for assembly in assemblies:
            downloader = RefSeqDownloader(
                assembly=assembly,
                base_url=base_url,
                output_folder=output_folder,
                max_concurrent=max_concurrent,
            )
            results[assembly] = downloader.download(
                files=files, force=force, **kwargs
            )
    return results

def download_refseq(
    assemblies: List[str],
    output_folder: Optional[Path] = None,
    files: Optional[List[str]] = None,
    base_url: str = DEFAULT_REFSEQ_URL,
    prefix: str = "ncbiRefSeq",
    max_concurrent: int = 3,
    format_to_bed: bool = True,
    **kwargs,
) -> Dict[str, Optional[Path]]:
    """Legacy wrapper for downloading RefSeq files."""
    results_by_assembly = download_refseq_multiple_assemblies(
        assemblies=assemblies,
        output_folder=output_folder,
        files=files,
        base_url=base_url,
        max_concurrent=max_concurrent,
        force=False,
        format_to_bed=format_to_bed,
        **kwargs,
    )

    # Flatten results for backward compatibility
    flat_results = {}
    for assembly_results in results_by_assembly.values():
        flat_results.update(assembly_results)

    return flat_results

def format_refseq_to_bed(
    refseq_file: Path,
    output_file: Path,
    include_utr_5: bool = True,
    include_utr_3: bool = True,
    include_chrm: bool = True,
    include_non_canonical_chr: bool = True,
    include_non_coding_transcripts: bool = True,
    include_transcript_ver: bool = True,
) -> None:
    """Format RefSeq file to BED format using biobear."""
    with start_action(
        action_type="format_refseq_to_bed",
        input_file=str(refseq_file),
        output_file=str(output_file),
    ):
        session = bb.connect()

        # Build filter conditions
        conditions = []
        if not include_chrm:
            conditions.append("chrom != 'chrM'")
        if not include_non_canonical_chr:
            conditions.append("chrom RLIKE '^chr[0-9XY]+$'")
        where_clause = " AND ".join(conditions) or "1=1"

        query = f"""
            SELECT
                chrom, CAST(txStart AS INTEGER), CAST(txEnd AS INTEGER),
                CASE WHEN {include_transcript_ver} THEN name ELSE SPLIT_PART(name, '.', 1) END,
                0, strand, CAST(cdsStart AS INTEGER), CAST(cdsEnd AS INTEGER), '0,0,0',
                CAST(exonCount AS INTEGER), exonStarts, exonEnds
            FROM read_csv(
                '{refseq_file}', delimiter='\t', header=false,
                column_names=['bin', 'name', 'chrom', 'strand', 'txStart', 'txEnd', 'cdsStart',
                              'cdsEnd', 'exonCount', 'exonStarts', 'exonEnds', 'score', 'name2',
                              'cdsStartStat', 'cdsEndStat', 'exonFrames']
            )
            WHERE {where_clause}
            ORDER BY chrom, txStart
        """
        result = session.sql(query)
        df = result.to_pandas()
        df.to_csv(output_file, sep="\t", index=False, header=False)
