from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import pooch
from eliot import start_action
from pydantic import BaseModel, Field, ConfigDict

from genobear.io import vcf_to_parquet


class DownloadResult(BaseModel):
    """
    Model representing the result of downloading a single VCF file.
    
    Contains paths to the VCF file and optional index/parquet files,
    plus a LazyFrame for efficient data access.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    vcf: Optional[Path] = Field(default=None, description="Path to the VCF file")
    index: Optional[Path] = Field(default=None, description="Path to the index file (.tbi or .csi)")
    parquet: Optional[Path] = Field(default=None, description="Path to the converted parquet file")
    lazy_frame: Optional[pl.LazyFrame] = Field(default=None, exclude=True, description="LazyFrame reading from parquet file")
    
    @property
    def has_parquet(self) -> bool:
        """Check if parquet file exists."""
        return self.parquet is not None and self.parquet.exists()
    
    @property
    def has_vcf(self) -> bool:
        """Check if VCF file exists."""
        return self.vcf is not None and self.vcf.exists()
    
    @property
    def has_index(self) -> bool:
        """Check if index file exists."""
        return self.index is not None and self.index.exists()
    
    def get_lazy_frame(self) -> pl.LazyFrame:
        """
        Get or create a LazyFrame from the parquet file.
        
        Returns:
            LazyFrame reading from the parquet file
            
        Raises:
            ValueError: If no parquet file is available
        """
        if not self.has_parquet:
            raise ValueError("No parquet file available. Ensure the VCF was converted to parquet.")
        
        if self.lazy_frame is None:
            self.lazy_frame = pl.scan_parquet(self.parquet)
        
        return self.lazy_frame


class MultiVCFDownloader(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    """
    Downloads multiple VCF files in parallel using pooch's native capabilities
    and optionally converts/merges them to parquet.
    
    This is useful for cases where data is split across multiple VCF files
    (e.g., per-chromosome files from Ensembl).
    """
    
    # Configuration - simple dict mapping identifiers to URLs
    vcf_urls: Dict[str, str] = Field(description="Mapping of identifier (e.g. chr1) to VCF URL")
    index_urls: Optional[Dict[str, str]] = Field(default=None, description="Optional mapping of identifier to index URLs")
    known_hashes: Optional[Dict[str, str]] = Field(default=None, description="Optional mapping of identifier to known hashes")
    
    cache_subdir: str = "multi_vcf_downloader"
    parallel: bool = Field(default=True, description="Download files concurrently using a thread pool")
    max_workers: int = Field(default=8, description="Maximum number of concurrent download threads")
    progressbar: bool = Field(default=True, description="Show per-file progress bars during downloads (may interleave when parallel)")
    streaming: bool = Field(default=False, description="Whether to use streaming mode when reading VCF files")
    clean_semicolons: bool = Field(default=False, description="Whether to clean malformed semicolons (;;, ;:, ;;:) before processing VCF files. Files are modified in-place, including decompression/recompression for .gz files.")
    
    # Output optionsf
    convert_to_parquet: bool = Field(default=True, description="Convert each VCF to parquet after download")
    merge_parquets: bool = Field(default=False, description="Merge all parquet files into one")
    merged_output_path: Optional[Path] = Field(default=None, description="Path for merged parquet file")
    clean_intermediates: bool = Field(default=False, description="Delete downloaded VCF and index files after converting to parquet (saves disk space)")
    
    # State (excluded from serialization)
    registry: Optional[pooch.Pooch] = Field(default=None, exclude=True)
    download_results: Optional[Dict[str, Union[Dict[str, Path], DownloadResult]]] = Field(default=None, exclude=True)
    
    @property
    def lazy_frames(self) -> Dict[str, pl.LazyFrame]:
        """
        Get a dictionary of all available LazyFrames from download results.
        
        Returns:
            Dictionary mapping identifiers to LazyFrames
            
        Raises:
            ValueError: If no files have been downloaded yet
        """
        if self.download_results is None:
            raise ValueError("No files downloaded yet. Call download_all() first.")
        
        frames = {}
        for identifier, result in self.download_results.items():
            if isinstance(result, DownloadResult) and result.has_parquet:
                frames[identifier] = result.get_lazy_frame()
        
        return frames
    
    @property
    def parquet_paths(self) -> Dict[str, Path]:
        """
        Get a dictionary of all available parquet file paths from download results.
        
        Returns:
            Dictionary mapping identifiers to parquet file paths
            
        Raises:
            ValueError: If no files have been downloaded yet
        """
        if self.download_results is None:
            raise ValueError("No files downloaded yet. Call download_all() first.")
        
        paths = {}
        for identifier, result in self.download_results.items():
            if isinstance(result, DownloadResult) and result.has_parquet:
                paths[identifier] = result.parquet
        
        return paths
    
    def model_post_init(self, __context: any) -> None:
        """Initialize the pooch registry with all the files."""
        self.registry = pooch.create(
            path=pooch.os_cache(self.cache_subdir),
            base_url="",  # We'll use full URLs
            registry={}
        )
        
        # Add VCF files to registry
        for identifier, url in self.vcf_urls.items():
            filename = Path(url).name
            known_hash = self.known_hashes.get(identifier) if self.known_hashes else None
            self.registry.registry[filename] = known_hash
            
        # Add index files to registry if provided
        if self.index_urls:
            for identifier, url in self.index_urls.items():
                filename = Path(url).name
                self.registry.registry[filename] = None  # Index files typically don't have hashes

    def _get_expected_parquet_path(self, identifier: str, vcf_filename: str, decompress: bool = True) -> Path:
        """
        Get the expected parquet path for a given VCF file.
        
        Args:
            identifier: The identifier for this file (e.g., 'chr1')
            vcf_filename: The VCF filename
            decompress: Whether VCF files are being decompressed
            
        Returns:
            Expected path where parquet file would be saved
        """
        # Determine the VCF path after download/decompression
        vcf_path = Path(self.registry.path) / vcf_filename
        if vcf_filename.endswith('.gz') and decompress:
            # If we're decompressing, the actual VCF name won't have .gz
            vcf_path = Path(self.registry.path) / vcf_filename[:-3]
        
        # Convert to parquet path (same logic as in io.py)
        return vcf_path.with_suffix('.parquet')
    
    def download_all(
        self,
        decompress: bool = True,
        download_index: bool = True,
        force: bool = False
    ) -> Dict[str, DownloadResult]:
        """
        Download all VCF files in parallel using pooch's native capabilities.
        
        Returns:
            Dictionary mapping identifiers to DownloadResult objects:
            {
                "chr1": DownloadResult(vcf=Path, index=Path, parquet=Path),
                "chr2": DownloadResult(vcf=Path, index=Path, parquet=Path),
                ...
            }
        """
        with start_action(
            action_type="multi_vcf_download",
            num_files=len(self.vcf_urls)
        ) as action:
            
            if self.registry is None:
                self.model_post_init(None)
            
            results: Dict[str, DownloadResult] = {}
            
            # Check for existing parquet files if convert_to_parquet is True and not forcing
            skipped_downloads = []
            if self.convert_to_parquet and not force:
                for identifier, url in self.vcf_urls.items():
                    filename = Path(url).name
                    expected_parquet = self._get_expected_parquet_path(identifier, filename, decompress)
                    
                    if expected_parquet.exists():
                        # Parquet exists, skip VCF download
                        results[identifier] = DownloadResult(parquet=expected_parquet)
                        results[identifier].lazy_frame = pl.scan_parquet(expected_parquet)
                        skipped_downloads.append(identifier)
                        
                        action.log(
                            message_type="info",
                            action="skip_download",
                            identifier=identifier,
                            reason="parquet_exists",
                            parquet_path=str(expected_parquet)
                        )
            
            # Prepare list of files to download for pooch
            files_to_download = []
            
            # Add VCF files (only those not skipped)
            for identifier, url in self.vcf_urls.items():
                if identifier in skipped_downloads:
                    continue
                    
                filename = Path(url).name
                processor = pooch.Decompress(name=filename.replace('.gz', '')) if decompress and filename.endswith('.gz') else None
                files_to_download.append((identifier, url, filename, 'vcf', processor))
            
            # Add index files if requested (only for non-skipped files)
            if download_index and self.index_urls:
                for identifier, url in self.index_urls.items():
                    if identifier in skipped_downloads:
                        continue
                        
                    filename = Path(url).name
                    files_to_download.append((identifier, url, filename, 'index', None))
            
            # Download all files (optionally in parallel)
            def _do_download(task: Tuple[str, str, str, str, Optional[object]]) -> Tuple[str, str, Path]:
                identifier, url, filename, file_type, processor = task
                with start_action(
                    action_type="download_single_file",
                    identifier=identifier,
                    file_type=file_type,
                    url=url,
                    filename=filename
                ) as download_action:
                    downloaded_path = Path(
                        pooch.retrieve(
                            url=url,
                            known_hash=self.registry.registry.get(filename) if not force else None,
                            fname=filename,
                            path=self.registry.path,
                            processor=processor,
                            progressbar=self.progressbar,
                        )
                    )
                    download_action.log(
                        message_type="info",
                        status="completed",
                        path=str(downloaded_path)
                    )
                    return identifier, file_type, downloaded_path

            if self.parallel and len(files_to_download) > 1:
                with start_action(action_type="parallel_downloads", workers=self.max_workers, tasks=len(files_to_download)):
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = {executor.submit(_do_download, task): task for task in files_to_download}
                        for future in as_completed(futures):
                            identifier, file_type, downloaded_path = future.result()
                            if identifier not in results:
                                results[identifier] = DownloadResult()
                            if file_type == 'vcf':
                                results[identifier].vcf = downloaded_path
                            elif file_type == 'index':
                                results[identifier].index = downloaded_path
            else:
                for task in files_to_download:
                    identifier, file_type, downloaded_path = _do_download(task)
                    if identifier not in results:
                        results[identifier] = DownloadResult()
                    if file_type == 'vcf':
                        results[identifier].vcf = downloaded_path
                    elif file_type == 'index':
                        results[identifier].index = downloaded_path
            
            self.download_results = results
            
            # Log summary of skipped downloads
            if skipped_downloads:
                action.log(
                    message_type="info",
                    skipped_count=len(skipped_downloads),
                    skipped_identifiers=skipped_downloads,
                    downloaded_count=len(files_to_download),
                    summary="Skipped downloading VCF files where parquet already exists"
                )
            
            # Convert to parquet if requested
            if self.convert_to_parquet:
                self._convert_all_to_parquet(results)
                
                # Clean intermediate files if requested
                if self.clean_intermediates:
                    self._clean_intermediate_files(results)
            
            # Merge parquets if requested
            if self.merge_parquets and self.convert_to_parquet:
                self._merge_parquet_files(results)
            
            return results
    
    def _convert_all_to_parquet(
        self,
        results: Dict[str, DownloadResult]
    ) -> None:
        """Convert all downloaded VCF files to parquet format."""
        with start_action(action_type="convert_all_to_parquet", num_files=len(results)) as action:
            
            for identifier, result in results.items():
                if result.vcf is not None:
                    with start_action(
                        action_type="convert_single_vcf_to_parquet",
                        identifier=identifier,
                        vcf_path=str(result.vcf)
                    ) as convert_action:
                        parquet_path, lazy_frame = self._convert_single_to_parquet(result.vcf)
                        result.parquet = parquet_path
                        result.lazy_frame = lazy_frame
                        convert_action.log(
                            message_type="info",
                            status="completed",
                            parquet_path=str(parquet_path)
                        )
    
    def _convert_single_to_parquet(
        self,
        vcf_path: Path
    ) -> tuple[Path, pl.LazyFrame]:
        """Convert a single VCF file to parquet format.
        
        Returns:
            Tuple of (parquet_path, lazy_frame)
        """
        # Use the dedicated vcf_to_parquet function
        lazy_frame, parquet_path = vcf_to_parquet(
            vcf_path,
            streaming=self.streaming,
            clean_semicolons=self.clean_semicolons
        )
        
        return parquet_path, lazy_frame
    
    def _clean_intermediate_files(
        self,
        results: Dict[str, DownloadResult]
    ) -> None:
        """Clean intermediate VCF and index files after parquet conversion."""
        with start_action(action_type="clean_intermediate_files", num_files=len(results)) as action:
            
            files_cleaned = 0
            files_failed = 0
            
            for identifier, result in results.items():
                # Only clean if parquet conversion was successful
                if result.has_parquet:
                    
                    # Also remove any implied VCF sitting next to the parquet even if it wasn't tracked
                    try:
                        implied_vcf = result.parquet.with_suffix('.vcf') if result.parquet is not None else None
                        if implied_vcf is not None and implied_vcf.exists():
                            implied_vcf.unlink()
                            action.log(
                                message_type="info",
                                identifier=identifier,
                                file_type="vcf",
                                cleaned_file=str(implied_vcf),
                                reason="implied_vcf_removed"
                            )
                            files_cleaned += 1
                    except Exception as e:
                        action.log(
                            message_type="error",
                            identifier=identifier,
                            file_type="vcf",
                            error=str(e),
                            failed_file=str(implied_vcf) if implied_vcf is not None else None
                        )
                        files_failed += 1

                    # Clean VCF file
                    if result.has_vcf:
                        try:
                            result.vcf.unlink()
                            action.log(
                                message_type="info",
                                identifier=identifier,
                                file_type="vcf",
                                cleaned_file=str(result.vcf)
                            )
                            files_cleaned += 1
                            # Update the model to reflect file no longer exists
                            result.vcf = None
                        except Exception as e:
                            action.log(
                                message_type="error",
                                identifier=identifier,
                                file_type="vcf",
                                error=str(e),
                                failed_file=str(result.vcf)
                            )
                            files_failed += 1
                    
                    # Clean index file
                    if result.has_index:
                        try:
                            result.index.unlink()
                            action.log(
                                message_type="info",
                                identifier=identifier,
                                file_type="index",
                                cleaned_file=str(result.index)
                            )
                            files_cleaned += 1
                            # Update the model to reflect file no longer exists
                            result.index = None
                        except Exception as e:
                            action.log(
                                message_type="error",
                                identifier=identifier,
                                file_type="index",
                                error=str(e),
                                failed_file=str(result.index)
                            )
                            files_failed += 1
                else:
                    action.log(
                        message_type="warning",
                        identifier=identifier,
                        reason="parquet_not_found",
                        message="Skipping cleanup - parquet file not found or conversion failed"
                    )
            
            action.log(
                message_type="info",
                files_cleaned=files_cleaned,
                files_failed=files_failed,
                cleanup_complete=True
            )
    
    def _merge_parquet_files(
        self,
        results: Dict[str, DownloadResult]
    ) -> Path:
        """Merge all parquet files into a single file."""
        with start_action(action_type="merge_parquet_files") as action:
            
            parquet_paths = []
            for identifier, result in results.items():
                if result.parquet is not None:
                    parquet_paths.append(result.parquet)
            
            if not parquet_paths:
                raise ValueError("No parquet files to merge")
            
            action.log(
                message_type="info",
                num_files=len(parquet_paths),
                files=[str(p) for p in parquet_paths]
            )
            
            # Read all parquet files lazily and concatenate
            lazy_frames = [pl.scan_parquet(path) for path in parquet_paths]
            merged_lf = pl.concat(lazy_frames, how="vertical_relaxed")
            
            # Determine output path
            if self.merged_output_path:
                output_path = self.merged_output_path
            else:
                # Default to cache directory
                output_path = Path(self.registry.path) / "merged.parquet"
            
            # Write the merged data
            merged_lf.collect().write_parquet(output_path)
            
            action.log(
                message_type="info",
                merged_path=str(output_path)
            )
            
            # Store the merged path in results
            if self.download_results:
                self.download_results["_merged"] = DownloadResult(parquet=output_path)
            
            return output_path
    
    @classmethod
    def from_url_list(
        cls,
        urls: List[str],
        cache_subdir: str = "multi_vcf_downloader",
        **kwargs
    ) -> "MultiVCFDownloader":
        """
        Create a MultiVCFDownloader from a simple list of URLs.
        
        This is a convenience method for cases where you just have URLs
        and don't need custom configuration for each file.
        """
        vcf_urls = {}
        index_urls = {}
        
        for url in urls:
            filename = Path(url).name
            # Use filename stem as identifier (e.g., chr1.vcf.gz -> chr1)
            identifier = Path(filename).stem.replace('.vcf', '')
            
            vcf_urls[identifier] = url
            
            # Assume .tbi index files for VCF files
            if url.endswith('.vcf.gz') or url.endswith('.vcf'):
                index_urls[identifier] = f"{url}.tbi"
        
        return cls(
            vcf_urls=vcf_urls, 
            index_urls=index_urls if index_urls else None,
            cache_subdir=cache_subdir, 
            **kwargs
        )
    
    def read_merged_parquet(self) -> pl.LazyFrame:
        """
        Read all downloaded and converted parquet files as a single LazyFrame/DataFrame.
        
        Args:
            return_lazy: If True, return a LazyFrame. If False, collect and return DataFrame.
            
        Returns:
            LazyFrame combining data from all parquet files
        """
        if self.download_results is None:
            raise ValueError("No files downloaded yet. Call download_all() first.")
        
        # Check if we already have a merged file
        if "_merged" in self.download_results:
            merged_result = self.download_results["_merged"]
            if isinstance(merged_result, DownloadResult) and merged_result.parquet is not None:
                lf = pl.scan_parquet(merged_result.parquet)
            else:
                # Handle legacy dict format for backward compatibility
                if isinstance(merged_result, dict) and "parquet" in merged_result:
                    lf = pl.scan_parquet(merged_result["parquet"])
                else:
                    raise ValueError("Merged result found but no parquet file available")
        else:
            # Collect all parquet paths
            parquet_paths = []
            for identifier, result in self.download_results.items():
                if identifier != "_merged":
                    if isinstance(result, DownloadResult) and result.parquet is not None:
                        parquet_paths.append(result.parquet)
                    # Handle legacy dict format for backward compatibility
                    elif isinstance(result, dict) and "parquet" in result:
                        parquet_paths.append(result["parquet"])
            
            if not parquet_paths:
                raise ValueError("No parquet files available. Ensure convert_to_parquet=True was used.")
            
            # Read and concatenate all parquet files
            lazy_frames = [pl.scan_parquet(path) for path in parquet_paths]
            lf = pl.concat(lazy_frames, how="vertical_relaxed")

        return lf
