"""
VCF-based database downloader with conversion utilities.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from abc import abstractmethod
from pathlib import Path
from typing import Dict, Optional, final
import polars as pl
from eliot import start_action
from pydantic import Field, field_validator

from genobear.databases.downloader import GenomicDatabaseDownloader
from genobear.io import read_vcf_file, SaveParquet


class VCFDatabaseDownloader(GenomicDatabaseDownloader):
    """Base class for VCF-based database downloaders (dbSNP, ClinVar, etc)."""
    
    base_url: str = Field(default="", description="Base URL for database downloads")
    
    @field_validator('base_url')
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Validate base URL format."""
        if v and not (v.startswith('http://') or v.startswith('https://') or v.startswith('ftp://')):
            raise ValueError('base_url must be a valid HTTP, HTTPS, or FTP URL')
        return v
    
    @abstractmethod
    def construct_url(self, version: Optional[str] = None, **kwargs) -> Optional[str]:
        """Construct the download URL for this database and assembly.

        Subclassing: Required to implement.
        """
        pass
    
    @staticmethod
    @final
    def read_vcf_with_compression_detection(
        vcf_path: Path,
        parse_info: bool = True,
        parse_formats: bool = False,
        info_fields: Optional[list[str]] = None
    ) -> pl.DataFrame:
        """
        Read VCF file with automatic compression detection using polars-bio.
        
        Args:
            vcf_path: Path to VCF file (compressed or uncompressed)
            session: Optional session (ignored, kept for compatibility)
            parse_info: Whether to parse INFO fields
            parse_formats: Whether to parse FORMAT fields (ignored, kept for compatibility)
            info_fields: Specific INFO fields to parse
            
        Returns:
            Polars DataFrame with VCF data
        """
        with start_action(
            action_type="read_vcf_with_compression_detection",
            vcf_path=str(vcf_path),
            parse_info=parse_info,
            parse_formats=parse_formats,
            info_fields=info_fields
        ) as action:
            
            try:
                action.log("Reading VCF file using polars-bio")
                
                # Use the utility function from genobear.utils.io
                result = read_vcf_file(
                    file_path=vcf_path,
                    info_fields=info_fields,
                    streaming=False  # Return DataFrame instead of LazyFrame
                )
                
                # Ensure we have a DataFrame
                if isinstance(result, pl.LazyFrame):
                    df = result.collect()
                else:
                    df = result
                
                action.log("Successfully read VCF file", rows=len(df), columns=len(df.columns))
                return df
                
            except Exception as e:
                action.log("Failed to read VCF file", error=str(e))
                raise
    
    @final
    def download_vcf_with_index(
        self,
        url: str,
        dest_folder: Path,
        force: bool = False
    ) -> Dict[str, Optional[Path]]:
        """Download VCF file and its .tbi index concurrently."""
        results = {}
        
        # Prepare download tasks
        filename = Path(url).name
        vcf_path = dest_folder / filename
        tbi_url = f"{url}.tbi"
        tbi_path = dest_folder / f"{filename}.tbi"
        
        download_tasks = []
        
        # Add VCF download if needed
        if not self.should_skip_download(vcf_path, force):
            download_tasks.append(('vcf', url, vcf_path))
        else:
            results['vcf'] = vcf_path
        
        # Add index download if needed
        if not self.should_skip_download(tbi_path, force):
            download_tasks.append(('index', tbi_url, tbi_path))
        else:
            results['index'] = tbi_path
        
        # Download files concurrently
        if download_tasks:
            with ThreadPoolExecutor(max_workers=min(len(download_tasks), self.max_concurrent)) as executor:
                # Submit all download tasks
                future_to_key = {
                    executor.submit(GenomicDatabaseDownloader.download_file, task_url, task_path): task_key
                    for task_key, task_url, task_path in download_tasks
                }
                
                # Collect results
                for future in as_completed(future_to_key):
                    task_key = future_to_key[future]
                    try:
                        result = future.result()
                        results[task_key] = result
                    except Exception as e:
                        print(f"Download failed for {task_key}: {e}")
                        results[task_key] = None
        
        return results
    
    @final
    def convert_to_parquet(
        self,
        vcf_path: Path,
        save_parquet: SaveParquet = "auto",
        streaming: bool = True,
        force: bool = False,
        **kwargs
    ) -> Optional[Path]:
        """
        Convert VCF file to Parquet format using read_vcf_file.
        
        This is a thin wrapper around read_vcf_file that focuses on the conversion aspect.
        
        Args:
            vcf_path: Path to input VCF file
            save_parquet: Where to save parquet file (None, "auto", or Path)
            streaming: Whether to use streaming mode
            force: Whether to overwrite existing parquet file
            **kwargs: Additional arguments passed to read_vcf_file
            
        Returns:
            Path to created Parquet file if successful, None if failed
        
        Subclassing: Do not override. If a database requires a different
        conversion flow, implement it in the concrete downloader by calling
        utilities from `genobear.io` directly.
        """
        if not vcf_path or not vcf_path.exists():
            return None
        
        # Handle force parameter - check if parquet already exists
        if save_parquet and isinstance(save_parquet, Path) and save_parquet.exists() and not force:
            return save_parquet
            
        with start_action(action_type="convert_vcf_to_parquet", 
                         input_file=str(vcf_path), 
                         save_parquet=str(save_parquet) if isinstance(save_parquet, Path) else save_parquet) as action:
            
            try:
                action.log("Converting VCF to Parquet using read_vcf_file")
                
                # Use read_vcf_file which handles all the conversion logic
                result = read_vcf_file(
                    file_path=vcf_path,
                    streaming=streaming,
                    save_parquet=save_parquet,
                    **kwargs
                )
                
                # Determine the created parquet path
                if isinstance(save_parquet, Path):
                    created_path = save_parquet
                elif save_parquet == "auto":
                    from genobear.io import _default_parquet_path
                    created_path = _default_parquet_path(vcf_path)
                else:
                    # save_parquet is None, no parquet was created
                    action.log("No parquet file requested")
                    return None
                
                if created_path.exists():
                    action.log("Successfully converted VCF to Parquet", 
                             output_path=str(created_path),
                             output_size=created_path.stat().st_size)
                    return created_path
                else:
                    action.log("Parquet file was not created")
                    return None
                    
            except Exception as e:
                action.log("Conversion failed", error=str(e))
                return None