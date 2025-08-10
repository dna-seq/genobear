import os
import urllib.request
from pathlib import Path
from typing import Literal, Optional

import pooch
from eliot import start_action, to_file
from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator
from pycomfort.logging import to_nice_stdout


class VCFDownloader(BaseModel):
    """
    A generic, base class to lazily download and decompress a VCF file and
    its corresponding index. It's a Pydantic model to ensure configuration
    is validated and type-safe.
    
    Uses pooch.retrieve() directly instead of managing Pooch instances,
    following best practices for simple downloads.
    """
    # --- Configuration Fields ---
    base_url: HttpUrl
    vcf_filename: str
    tbi_filename: str
    cache_subdir: str
    hash_filename: Optional[str] = None
    known_hash: Optional[str] = None
    streaming: bool = Field(default=False, description="Whether to use streaming mode when reading VCF files")

    # --- Private, State-related Attributes ---
    vcf_path_cached: Optional[Path] = Field(default=None, exclude=True)
    tbi_path_cached: Optional[Path] = Field(default=None, exclude=True)
    vcf_hash_cached: Optional[str] = Field(default=None, exclude=True)

    def model_post_init(self, __context: any) -> None:
        """
        Pydantic v2 hook to run after model initialization.
        Resolves the hash if needed.
        """
        with start_action(action_type="downloader_init", cache_subdir=self.cache_subdir) as action:
            if not self.known_hash and self.hash_filename:
                try:
                    hash_url = f"{self.base_url}{self.hash_filename}"
                    self.vcf_hash_cached = self._get_remote_hash(hash_url)
                    action.log(message_type="info", hash_found=True, source=hash_url)
                except Exception as e:
                    action.log(message_type="warning", hash_found=False, error=str(e))
                    self.vcf_hash_cached = None
            else:
                self.vcf_hash_cached = self.known_hash
            
            action.log(message_type="info", downloader_initialized=True, has_hash=bool(self.vcf_hash_cached))

    @property
    def vcf_path(self) -> Path:
        """Lazy-loads the VCF file path. Fetches if not already downloaded."""
        if self.vcf_path_cached is None:
            self.fetch_files()
        return self.vcf_path_cached

    @property
    def tbi_path(self) -> Optional[Path]:
        """Lazy-loads the TBI file path. Fetches if not already downloaded."""
        if self.tbi_path_cached is None:
            self.fetch_files()
        return self.tbi_path_cached

    def _get_remote_hash(self, hash_url: str) -> str:
        """Fetches a hash from a remote URL."""
        with urllib.request.urlopen(hash_url) as response:
            hash_value = response.read().decode("utf-8").strip().split()[0]
        return f"md5:{hash_value}"

    def fetch_files(self, decompress: bool = True, download_index: bool = True, force: bool = False) -> None:
        """
        Fetches and optionally decompresses the VCF and TBI files using pooch.retrieve().
        
        Args:
            decompress: Whether to decompress the VCF file
            download_index: Whether to download the index (.tbi) file
            force: Whether to force redownload even if files exist (sets known_hash=None)
        """
        if self.vcf_path_cached and (not download_index or self.tbi_path_cached) and not force:
            return

        with start_action(
            action_type="download_vcf",
            config=self.model_dump(mode='json'),
            force=force
        ) as action:
            
            # Determine hash to use (None for force download)
            hash_to_use = None if force else self.vcf_hash_cached
            
            # Setup cache path
            cache_path = pooch.os_cache(self.cache_subdir)
            
            # Configure processor for decompression
            if decompress:
                # Create a clean name without .gz extension for the decompressed file
                if self.vcf_filename.endswith('.gz'):
                    clean_name = self.vcf_filename[:-3]  # Remove .gz
                else:
                    clean_name = self.vcf_filename + '.vcf'
                processor = pooch.Decompress(name=clean_name)
            else:
                processor = None
            
            action.log(message_type="info", step="fetching_vcf", force=force, has_hash=bool(hash_to_use))
            
            # Download VCF file using pooch.retrieve()
            vcf_url = f"{self.base_url}{self.vcf_filename}"
            self.vcf_path_cached = Path(pooch.retrieve(
                url=vcf_url,
                known_hash=hash_to_use,
                fname=self.vcf_filename,
                path=cache_path,
                processor=processor,
                progressbar=True
            ))

            if download_index:
                action.log(message_type="info", step="fetching_tbi", force=force)
                
                # Download TBI file (no hash, no decompression)
                tbi_url = f"{self.base_url}{self.tbi_filename}"
                self.tbi_path_cached = Path(pooch.retrieve(
                    url=tbi_url,
                    known_hash=None if force else None,  # TBI files typically don't have hashes
                    fname=self.tbi_filename,
                    path=cache_path,
                    processor=None,  # TBI files should not be decompressed
                    progressbar=True
                ))

    def read_vcf(self, **kwargs):
        """
        Read the downloaded VCF file using the downloader's configuration.
        
        Args:
            **kwargs: Additional arguments to pass to read_vcf_file, 
                     will override downloader defaults where specified.
        
        Returns:
            Polars LazyFrame or DataFrame containing the VCF data.
        """
        from genobear.io import read_vcf_file
        
        # Set default streaming from downloader config, but allow override
        if 'streaming' not in kwargs:
            kwargs['streaming'] = self.streaming
            
        # Ensure we have the VCF path
        vcf_path = self.vcf_path
        
        return read_vcf_file(vcf_path, **kwargs)
