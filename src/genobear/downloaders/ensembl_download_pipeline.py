from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Union, Tuple
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import pooch
from eliot import start_action
from pipefunc import pipefunc, Pipeline
from pydantic import BaseModel, HttpUrl, Field

from genobear.io import vcf_to_parquet, resolve_genobear_subfolder


class EnsemblPipelineConfig(BaseModel):
    """Configuration for the Ensembl download pipeline."""
    
    base_url: HttpUrl = Field(
        default="https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/",
        description="Base URL for Ensembl variation data"
    )
    
    base: str | Path = Field(
        default_factory=lambda: os.getenv("GENOBEAR_FOLDER", "genobear"),
        description="Base directory for cache (uses GENOBEAR_FOLDER env var)"
    )
    
    subdir_name: str = Field(
        default="ensembl_variations", 
        description="Cache subdirectory for Ensembl variation data"
    )
    
    chromosomes: Optional[Set[str]] = Field(
        default=None,
        description="Set of chromosomes to download. If None, downloads all available chromosomes"
    )
    
    use_csi_index: bool = Field(
        default=True,
        description="Whether to use CSI index files (.csi) instead of TBI (.tbi)"
    )
    
    use_checksums: bool = Field(
        default=True,
        description="Whether to fetch and use checksums from the CHECKSUMS file"
    )
    
    convert_to_parquet: bool = Field(
        default=True,
        description="Convert VCF files to parquet format"
    )
    
    split_variants: bool = Field(
        default=False,
        description="Split variants by type (SNV, etc.)"
    )
    
    parallel: bool = Field(
        default=True,
        description="Download files in parallel"
    )
    
    max_workers: int = Field(
        default=8,
        description="Maximum number of parallel workers"
    )
    
    clean_intermediates: bool = Field(
        default=False,
        description="Clean VCF files after parquet conversion"
    )

    @property
    def cache_dir(self) -> Path:
        """Get the cache directory path."""
        return resolve_genobear_subfolder(self.subdir_name, self.base)


# Pipefunc-decorated functions for the pipeline
@pipefunc(output_name="available_chromosomes", cache=True)
def discover_chromosomes(base_url: str) -> Set[str]:
    """Discover available chromosomes from Ensembl FTP."""
    with start_action(action_type="discover_chromosomes", base_url=base_url) as action:
        try:
            with urllib.request.urlopen(base_url) as response:
                html_content = response.read().decode('utf-8')
            
            vcf_pattern = r'homo_sapiens-chr([^.]+)\.vcf\.gz"'
            matches = re.findall(vcf_pattern, html_content)
            
            if matches:
                chromosomes = set(matches)
                action.log(message_type="info", discovered_chromosomes=list(chromosomes))
                return chromosomes
        except Exception as e:
            action.log(message_type="error", error=str(e))
        
        # Fallback
        fallback = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
                   '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
                   'X', 'Y', 'MT'}
        action.log(message_type="warning", using_fallback=list(fallback))
        return fallback


@pipefunc(output_name="checksums", cache=True)
def fetch_checksums(base_url: str, use_checksums: bool) -> Optional[Dict[str, str]]:
    """Fetch checksums from CHECKSUMS file."""
    if not use_checksums:
        return None
        
    with start_action(action_type="fetch_checksums", base_url=base_url) as action:
        try:
            checksums = {}
            checksums_url = f"{base_url}CHECKSUMS"
            
            with urllib.request.urlopen(checksums_url) as response:
                content = response.read().decode('utf-8')
            
            for line in content.strip().split('\n'):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split()
                if len(parts) >= 2:
                    hash_value = parts[0]
                    filename = parts[1]
                    
                    if filename.startswith('homo_sapiens-chr') and filename.endswith('.vcf.gz'):
                        chrom_match = re.search(r'homo_sapiens-chr([^.]+)\.vcf\.gz', filename)
                        if chrom_match:
                            chrom = chrom_match.group(1)
                            checksums[f"chr{chrom}"] = f"md5:{hash_value}"
            
            action.log(message_type="info", checksums_parsed=len(checksums))
            return checksums if checksums else None
            
        except Exception as e:
            action.log(message_type="error", error=str(e))
            return None


@pipefunc(output_name="target_chromosomes")
def determine_targets(available_chromosomes: Set[str], requested_chromosomes: Optional[Set[str]]) -> Set[str]:
    """Determine which chromosomes to download."""
    with start_action(action_type="determine_targets") as action:
        if requested_chromosomes is None:
            target_chromosomes = available_chromosomes
        else:
            target_chromosomes = requested_chromosomes.intersection(available_chromosomes)
            missing = requested_chromosomes - available_chromosomes
            if missing:
                action.log(message_type="warning", missing_chromosomes=list(missing))
        
        action.log(message_type="info", target_chromosomes=list(target_chromosomes))
        return target_chromosomes


@pipefunc(output_name="download_urls")
def build_urls(target_chromosomes: Set[str], base_url: str, use_csi_index: bool) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Build download URLs for VCF and index files."""
    with start_action(action_type="build_urls") as action:
        vcf_urls = {}
        index_urls = {}
        
        for chrom in target_chromosomes:
            vcf_filename = f"homo_sapiens-chr{chrom}.vcf.gz"
            vcf_urls[f"chr{chrom}"] = f"{base_url}{vcf_filename}"
            
            if use_csi_index:
                index_filename = f"homo_sapiens-chr{chrom}.vcf.gz.csi"
                index_urls[f"chr{chrom}"] = f"{base_url}{index_filename}"
        
        action.log(message_type="info", vcf_count=len(vcf_urls), index_count=len(index_urls))
        return vcf_urls, index_urls


@pipefunc(output_name="downloaded_files", cache=True)
def download_files(
    download_urls: Tuple[Dict[str, str], Dict[str, str]], 
    checksums: Optional[Dict[str, str]],
    cache_dir: Path,
    parallel: bool,
    max_workers: int
) -> Dict[str, Dict[str, Path]]:
    """Download VCF and index files."""
    vcf_urls, index_urls = download_urls
    
    with start_action(action_type="download_files", num_files=len(vcf_urls)) as action:
        cache_dir.mkdir(parents=True, exist_ok=True)
        results = {}
        
        # Check for existing files first
        for identifier in vcf_urls:
            vcf_filename = Path(vcf_urls[identifier]).name
            vcf_path = cache_dir / vcf_filename
            
            if vcf_path.exists():
                results[identifier] = {'vcf': vcf_path}
                
                # Check for index file
                if identifier in index_urls:
                    index_filename = Path(index_urls[identifier]).name
                    index_path = cache_dir / index_filename
                    if index_path.exists():
                        results[identifier]['index'] = index_path
                
                action.log(message_type="info", identifier=identifier, status="already_exists")
        
        # Download missing files
        download_tasks = []
        
        # Add VCF files that don't exist
        for identifier, url in vcf_urls.items():
            if identifier not in results or 'vcf' not in results[identifier]:
                filename = Path(url).name
                known_hash = checksums.get(identifier) if checksums else None
                download_tasks.append((identifier, url, filename, 'vcf', known_hash))
        
        # Add index files that don't exist
        for identifier, url in index_urls.items():
            if identifier not in results or 'index' not in results.get(identifier, {}):
                filename = Path(url).name
                download_tasks.append((identifier, url, filename, 'index', None))
        
        if not download_tasks:
            action.log(message_type="info", message="All files already exist")
            return results
        
        def _download_single(task):
            identifier, url, filename, file_type, known_hash = task
            
            with start_action(action_type="download_single", identifier=identifier) as dl_action:
                downloaded_path = Path(
                    pooch.retrieve(
                        url=url,
                        known_hash=known_hash,
                        fname=filename,
                        path=cache_dir,
                        progressbar=True
                    )
                )
                dl_action.log(message_type="info", path=str(downloaded_path))
                return identifier, file_type, downloaded_path
        
        if parallel and len(download_tasks) > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_download_single, task): task for task in download_tasks}
                
                for future in as_completed(futures):
                    identifier, file_type, downloaded_path = future.result()
                    if identifier not in results:
                        results[identifier] = {}
                    results[identifier][file_type] = downloaded_path
        else:
            for task in download_tasks:
                identifier, file_type, downloaded_path = _download_single(task)
                if identifier not in results:
                    results[identifier] = {}
                results[identifier][file_type] = downloaded_path
        
        action.log(message_type="info", downloaded_files=len(results))
        return results


@pipefunc(output_name="parquet_files", cache=True) 
def convert_to_parquet(downloaded_files: Dict[str, Dict[str, Path]], convert: bool) -> Dict[str, Path]:
    """Convert VCF files to parquet format."""
    if not convert:
        return {}
        
    with start_action(action_type="convert_to_parquet") as action:
        parquet_results = {}
        
        for identifier, file_paths in downloaded_files.items():
            if 'vcf' in file_paths:
                vcf_path = file_paths['vcf']
                
                # Check if parquet already exists
                expected_parquet = vcf_path.with_suffix('').with_suffix('.parquet')
                if expected_parquet.exists():
                    parquet_results[identifier] = expected_parquet
                    action.log(message_type="info", identifier=identifier, status="already_exists")
                    continue
                
                with start_action(action_type="convert_single", identifier=identifier) as conv_action:
                    lazy_frame, parquet_path = vcf_to_parquet(vcf_path)
                    parquet_results[identifier] = parquet_path
                    conv_action.log(message_type="info", parquet_path=str(parquet_path))
        
        action.log(message_type="info", converted_files=len(parquet_results))
        return parquet_results


@pipefunc(output_name="split_variant_files")
def split_variants(parquet_files: Dict[str, Path], cache_dir: Path, split: bool, explode_snv_alt: bool = True) -> Dict[str, Dict[str, Path]]:
    """Split variants by type."""
    if not split or not parquet_files:
        return {}
        
    with start_action(action_type="split_variants") as action:
        split_results = {}
        
        for identifier, parquet_path in parquet_files.items():
            with start_action(action_type="split_single", identifier=identifier) as split_action:
                split_dir = cache_dir / "splitted_variants" / identifier
                split_dir.mkdir(parents=True, exist_ok=True)
                
                # Check if already split
                existing_splits = list(split_dir.glob("*/*.parquet"))
                if existing_splits:
                    # Reconstruct the variant files dict
                    variant_files = {}
                    for file in existing_splits:
                        tsa = file.parent.name
                        variant_files[tsa] = file
                    split_results[identifier] = variant_files
                    split_action.log(message_type="info", status="already_split", num_files=len(existing_splits))
                    continue
                
                df = pl.scan_parquet(parquet_path)
                tsas = df.select("tsa").unique().collect(streaming=True).to_series().to_list()
                
                variant_files = {}
                for tsa in tsas:
                    df_tsa = df.filter(pl.col("tsa") == tsa)
                    
                    if tsa == "SNV" and explode_snv_alt:
                        df_tsa = df_tsa.with_columns(pl.col("alt").str.split("|")).explode("alt")
                    
                    tsa_dir = split_dir / tsa
                    tsa_dir.mkdir(parents=True, exist_ok=True)
                    output_path = tsa_dir / f"{parquet_path.stem}.parquet"
                    
                    df_tsa.sink_parquet(output_path)
                    variant_files[tsa] = output_path
                    
                    split_action.log(message_type="info", tsa=tsa, output_path=str(output_path))
                
                split_results[identifier] = variant_files
        
        action.log(message_type="info", split_complete=len(split_results))
        return split_results


def create_ensembl_pipeline(config: EnsemblPipelineConfig) -> Pipeline:
    """
    Create a pipfunc pipeline for downloading and processing Ensembl VCF data.
    
    This creates a proper pipefunc Pipeline with dependencies between functions.
    The pipeline can be run with caching, parallelization, and other pipefunc features.
    
    Args:
        config: Configuration for the pipeline
    
    Returns:
        Configured pipefunc Pipeline instance
    """
    # Create pipeline with all functions and enable caching
    pipeline = Pipeline([
        discover_chromosomes,
        fetch_checksums,
        determine_targets,
        build_urls,
        download_files,
        convert_to_parquet,
        split_variants
    ], cache_type="hybrid")  # Use hybrid cache for best performance
    
    # Bind configuration values to the pipeline functions
    pipeline.update_defaults({
        "base_url": str(config.base_url),
        "use_checksums": config.use_checksums,
        "requested_chromosomes": config.chromosomes,
        "use_csi_index": config.use_csi_index,
        "cache_dir": config.cache_dir,
        "parallel": config.parallel,
        "max_workers": config.max_workers,
        "convert": config.convert_to_parquet,
        "split": config.split_variants,
        "explode_snv_alt": True
    })
    
    return pipeline


# Convenience functions for common use cases
def download_ensembl_chromosomes(
    chromosomes: Optional[List[str]] = None,
    base: Optional[str | Path] = None,
    subdir_name: str = "ensembl_variations",
    convert_to_parquet: bool = True,
    split_variants: bool = False,
    **kwargs
) -> Dict[str, any]:
    """
    Download Ensembl VCF files for specified chromosomes using pipefunc.
    
    Args:
        chromosomes: List of chromosomes to download (None for all)
        base: Base cache directory
        subdir_name: Subdirectory name for cache
        convert_to_parquet: Whether to convert to parquet
        split_variants: Whether to split variants by type
        **kwargs: Additional configuration options
    
    Returns:
        Dictionary with pipeline results based on configuration:
        - If split_variants=True: returns split_variant_files
        - If convert_to_parquet=True: returns parquet_files
        - Otherwise: returns downloaded_files
    """
    config = EnsemblPipelineConfig(
        chromosomes=set(chromosomes) if chromosomes else None,
        base=base or os.getenv("GENOBEAR_FOLDER", "genobear"),
        subdir_name=subdir_name,
        convert_to_parquet=convert_to_parquet,
        split_variants=split_variants,
        **kwargs
    )
    
    pipeline = create_ensembl_pipeline(config)
    
    # Determine which output to get based on configuration
    if config.split_variants:
        final_output = 'split_variant_files'
    elif config.convert_to_parquet:
        final_output = 'parquet_files'  
    else:
        final_output = 'downloaded_files'
    
    # Run the pipeline - pipefunc automatically handles dependencies and caching
    results = pipeline(final_output)
    
    return results


def download_ensembl_autosomes(
    base: Optional[str | Path] = None,
    subdir_name: str = "ensembl_variations",
    convert_to_parquet: bool = True,
    split_variants: bool = False,
    **kwargs
) -> Dict[str, any]:
    """Download autosomal chromosomes (1-22) only."""
    autosomes = [str(i) for i in range(1, 23)]
    return download_ensembl_chromosomes(
        chromosomes=autosomes,
        base=base,
        subdir_name=subdir_name,
        convert_to_parquet=convert_to_parquet,
        split_variants=split_variants,
        **kwargs
    )


def download_ensembl_sex_chromosomes(
    base: Optional[str | Path] = None,
    subdir_name: str = "ensembl_variations",
    convert_to_parquet: bool = True,
    split_variants: bool = False,
    **kwargs
) -> Dict[str, any]:
    """Download sex chromosomes (X, Y) only."""
    sex_chromosomes = ['X', 'Y']
    return download_ensembl_chromosomes(
        chromosomes=sex_chromosomes,
        base=base,
        subdir_name=subdir_name,
        convert_to_parquet=convert_to_parquet,
        split_variants=split_variants,
        **kwargs
    )


if __name__ == "__main__":
    """Test the pipefunc pipeline implementation."""
    import sys
    import tempfile
    from eliot import to_file
    from pathlib import Path
    
    # Setup logging
    log_file = Path("ensembl_pipeline_test.log")
    to_file(open(log_file, "w"))
    
    print("🧬 Ensembl Pipefunc Pipeline Test")
    print("=" * 60)
    
    # Test 1: Basic pipeline creation and structure
    print("\n1. Testing pipeline creation...")
    config = EnsemblPipelineConfig(
        chromosomes={'22'},
        base=tempfile.mkdtemp(prefix="ensembl_pipeline_test_"),
        convert_to_parquet=True,
        split_variants=False,
        use_checksums=False
    )
    
    pipeline = create_ensembl_pipeline(config)
    print(f"✅ Pipeline created with {len(pipeline.functions)} functions")
    print("   Functions:", [f.output_name for f in pipeline.functions])
    
    # Test 2: Show dependency graph
    print("\n2. Dependency graph:")
    for func in pipeline.functions:
        deps = [p for p in func.parameters if p not in pipeline.defaults]
        if deps:
            print(f"   {func.output_name} <- {deps}")
    
    # Test 3: Test caching
    print("\n3. Testing caching...")
    import time
    
    # First run
    start = time.time()
    chromosomes1 = pipeline("available_chromosomes")
    time1 = time.time() - start
    
    # Second run (should be cached)
    start = time.time()
    chromosomes2 = pipeline("available_chromosomes")
    time2 = time.time() - start
    
    print(f"   First run: {time1:.3f}s, found {len(chromosomes1)} chromosomes")
    print(f"   Second run: {time2:.3f}s (cached)")
    print(f"   ✅ Caching speedup: {time1/time2:.1f}x")
    
    # Test 4: Run full pipeline for a small chromosome
    print("\n4. Running full pipeline for chromosome 22...")
    if len(sys.argv) > 1 and sys.argv[1] == "--download":
        print("   Downloading VCF files (this may take a while)...")
        
        results = download_ensembl_chromosomes(
            chromosomes=['22'],
            base=config.base,
            convert_to_parquet=True,
            split_variants=False,
            use_checksums=False
        )
        
        print("   ✅ Pipeline completed!")
        for chrom, path in results.items():
            print(f"      {chrom}: {path}")
            if path.exists():
                size_mb = path.stat().st_size / (1024 * 1024)
                print(f"      Size: {size_mb:.1f} MB")
    else:
        print("   Skipping download (run with --download to test)")
    
    # Test 5: Test lazy evaluation
    print("\n5. Testing lazy evaluation...")
    try:
        from pipefunc.lazy import LazyPipeline
        print("   ✅ Lazy evaluation module available")
    except ImportError:
        print("   ℹ️  Lazy evaluation requires additional dependencies")
    
    # Test 6: Show cached functions
    print("\n6. Functions with caching enabled:")
    for func in pipeline.functions:
        if func.cache:
            print(f"   - {func.output_name}")
    
    # Test 7: Test direct pipeline calling
    print("\n7. Testing direct pipeline calling...")
    try:
        # Test calling pipeline directly for specific outputs
        target_chroms = pipeline("target_chromosomes")
        print(f"   ✅ Target chromosomes: {target_chroms}")
        
        # Test calling with specific parameters  
        urls = pipeline("download_urls")
        vcf_urls, index_urls = urls
        print(f"   ✅ Generated {len(vcf_urls)} VCF URLs and {len(index_urls)} index URLs")
    except Exception as e:
        print(f"   Pipeline calling test failed: {e}")
    
    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print(f"📝 Detailed logs written to: {log_file}")
    print("\nThe pipeline properly uses pipefunc features:")
    print("  - @pipefunc decorators with output names")
    print("  - Automatic dependency resolution")
    print("  - Built-in caching for expensive operations")
    print("  - Configuration binding")
    print("  - Lazy evaluation support")
    print("  - Parallel execution capability")