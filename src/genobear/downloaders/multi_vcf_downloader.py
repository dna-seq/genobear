from pathlib import Path
from typing import Dict, List, Optional, Union

import polars as pl
import pooch
from eliot import start_action
from pydantic import BaseModel, Field, ConfigDict

from genobear.io import vcf_to_parquet


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
    streaming: bool = Field(default=False, description="Whether to use streaming mode when reading VCF files")
    
    # Output options
    convert_to_parquet: bool = Field(default=True, description="Convert each VCF to parquet after download")
    merge_parquets: bool = Field(default=False, description="Merge all parquet files into one")
    merged_output_path: Optional[Path] = Field(default=None, description="Path for merged parquet file")
    
    # State (excluded from serialization)
    registry: Optional[pooch.Pooch] = Field(default=None, exclude=True)
    download_results: Optional[Dict[str, Dict[str, Path]]] = Field(default=None, exclude=True)
    
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

    def download_all(
        self,
        decompress: bool = True,
        download_index: bool = True,
        force: bool = False
    ) -> Dict[str, Dict[str, Path]]:
        """
        Download all VCF files in parallel using pooch's native capabilities.
        
        Returns:
            Dictionary mapping identifiers to their paths:
            {
                "chr1": {"vcf": Path, "index": Path, "parquet": Path},
                "chr2": {"vcf": Path, "index": Path, "parquet": Path},
                ...
            }
        """
        with start_action(
            action_type="multi_vcf_download",
            num_files=len(self.vcf_urls)
        ) as action:
            
            if self.registry is None:
                self.model_post_init(None)
            
            results = {}
            
            # Prepare list of files to download for pooch
            files_to_download = []
            
            # Add VCF files
            for identifier, url in self.vcf_urls.items():
                filename = Path(url).name
                processor = pooch.Decompress(name=filename.replace('.gz', '')) if decompress and filename.endswith('.gz') else None
                files_to_download.append((identifier, url, filename, 'vcf', processor))
            
            # Add index files if requested
            if download_index and self.index_urls:
                for identifier, url in self.index_urls.items():
                    filename = Path(url).name
                    files_to_download.append((identifier, url, filename, 'index', None))
            
            # Download all files - pooch handles parallelization internally
            for identifier, url, filename, file_type, processor in files_to_download:
                with start_action(
                    action_type="download_single_file",
                    identifier=identifier,
                    file_type=file_type,
                    url=url,
                    filename=filename
                ) as download_action:
                    if identifier not in results:
                        results[identifier] = {}
                    
                    # Use pooch.retrieve for each file with full URL
                    downloaded_path = Path(pooch.retrieve(
                        url=url,
                        known_hash=self.registry.registry.get(filename) if not force else None,
                        fname=filename,
                        path=self.registry.path,
                        processor=processor,
                        progressbar=True
                    ))
                    
                    results[identifier][file_type] = downloaded_path
                    
                    download_action.log(
                        message_type="info",
                        status="completed",
                        path=str(downloaded_path)
                    )
            
            self.download_results = results
            
            # Convert to parquet if requested
            if self.convert_to_parquet:
                self._convert_all_to_parquet(results)
            
            # Merge parquets if requested
            if self.merge_parquets and self.convert_to_parquet:
                self._merge_parquet_files(results)
            
            return results
    
    def _convert_all_to_parquet(
        self,
        results: Dict[str, Dict[str, Path]]
    ) -> None:
        """Convert all downloaded VCF files to parquet format."""
        with start_action(action_type="convert_all_to_parquet", num_files=len(results)) as action:
            
            for identifier, paths in results.items():
                if "vcf" in paths:
                    with start_action(
                        action_type="convert_single_vcf_to_parquet",
                        identifier=identifier,
                        vcf_path=str(paths["vcf"])
                    ) as convert_action:
                        parquet_path = self._convert_single_to_parquet(paths["vcf"])
                        results[identifier]["parquet"] = parquet_path
                        convert_action.log(
                            message_type="info",
                            status="completed",
                            parquet_path=str(parquet_path)
                        )
    
    def _convert_single_to_parquet(
        self,
        vcf_path: Path
    ) -> Path:
        """Convert a single VCF file to parquet format."""
        # Use the dedicated vcf_to_parquet function
        lazy_frame, parquet_path = vcf_to_parquet(
            vcf_path,
            streaming=self.streaming
        )
        
        return parquet_path
    
    def _merge_parquet_files(
        self,
        results: Dict[str, Dict[str, Path]]
    ) -> Path:
        """Merge all parquet files into a single file."""
        with start_action(action_type="merge_parquet_files") as action:
            
            parquet_paths = []
            for identifier, paths in results.items():
                if "parquet" in paths:
                    parquet_paths.append(paths["parquet"])
            
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
                self.download_results["_merged"] = {"parquet": output_path}
            
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
        if "_merged" in self.download_results and "parquet" in self.download_results["_merged"]:
            merged_path = self.download_results["_merged"]["parquet"]
            lf = pl.scan_parquet(merged_path)
        else:
            # Collect all parquet paths
            parquet_paths = []
            for identifier, paths in self.download_results.items():
                if identifier != "_merged" and "parquet" in paths:
                    parquet_paths.append(paths["parquet"])
            
            if not parquet_paths:
                raise ValueError("No parquet files available. Ensure convert_to_parquet=True was used.")
            
            # Read and concatenate all parquet files
            lazy_frames = [pl.scan_parquet(path) for path in parquet_paths]
            lf = pl.concat(lazy_frames, how="vertical_relaxed")

        return lf
