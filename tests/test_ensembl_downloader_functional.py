"""Functional tests for EnsemblDownloader that actually download and process files."""

import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Generator

import polars as pl
import pytest
from genobear.downloaders.ensembl_downloader import EnsemblDownloader


class TestEnsemblDownloaderFunctional:
    """Functional tests that actually download files and test the complete workflow."""
    
    @pytest.fixture
    def temp_cache_dir(self, use_shared_pooch_cache: bool, shared_pooch_cache_dir: Path) -> Generator[Optional[str], None, None]:
        """Provide a cache directory for tests.

        By default, reuse the shared pooch cache to avoid re-downloading.
        When --no-shared-pooch-cache is passed, use a temporary directory and clean it.
        """
        if use_shared_pooch_cache:
            # Use downloader's default shared cache; do not override
            yield None
        else:
            temp_dir: str = tempfile.mkdtemp()
            try:
                yield temp_dir
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_download_single_chromosome_with_parquet(self, temp_cache_dir: Optional[str]) -> None:
        """Test downloading a single chromosome and converting to parquet."""
        # Use chromosome 22 (smallest autosome for faster test)
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['22'],
            convert_to_parquet=True,
            merge_parquets=False
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download the files
        results: Dict[str, Dict[str, Path]] = downloader.download_all()
        
        # Verify results structure
        assert 'chr22' in results
        assert 'vcf' in results['chr22']
        assert 'index' in results['chr22']
        assert 'parquet' in results['chr22']
        
        # Verify files exist
        vcf_path: Path = results['chr22']['vcf']
        index_path: Path = results['chr22']['index']
        parquet_path: Path = results['chr22']['parquet']
        
        assert vcf_path.exists()
        assert index_path.exists() 
        assert parquet_path.exists()
        
        # Verify file types and sizes (allow .vcf if decompressed by downloader)
        assert vcf_path.suffix in {'.gz', '.vcf'}
        assert index_path.suffix == '.csi'
        assert parquet_path.suffix == '.parquet'
        
        # Files should be non-empty
        assert vcf_path.stat().st_size > 0
        assert index_path.stat().st_size > 0
        assert parquet_path.stat().st_size > 0
        
        print(f"Downloaded VCF: {vcf_path} ({vcf_path.stat().st_size:,} bytes)")
        print(f"Downloaded index: {index_path} ({index_path.stat().st_size:,} bytes)")
        print(f"Generated parquet: {parquet_path} ({parquet_path.stat().st_size:,} bytes)")
    
    def test_read_parquet_data(self, temp_cache_dir: Optional[str]) -> None:
        """Test that we can read and analyze the generated parquet files."""
        # Download chromosome 22
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['22'],
            convert_to_parquet=True
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download files
        results: Dict[str, Dict[str, Path]] = downloader.download_all()
        parquet_path: Path = results['chr22']['parquet']
        
        # Read parquet file
        df: pl.LazyFrame = pl.scan_parquet(parquet_path)
        
        # Basic structure checks
        schema = df.collect_schema()
        print(f"Parquet schema: {list(schema.keys())}")
        
        # Should have standard VCF columns
        expected_columns: List[str] = ['chromosome', 'position', 'reference', 'alternate']
        for col in expected_columns:
            assert col in schema, f"Missing expected column: {col}"
        
        # Count variants
        variant_count: int = df.select(pl.count()).collect().item()
        print(f"Number of variants in chromosome 22: {variant_count:,}")
        
        # Should have some variants
        assert variant_count > 0, "Parquet file should contain variants"
        assert variant_count > 100000, "Chromosome 22 should have many variants"
        
        # Check data quality - sample a few rows
        sample_data = df.limit(5).collect()
        print(f"Sample data:\n{sample_data}")
        
        # All chromosomes should be 22
        unique_chroms = df.select(pl.col('chromosome').unique()).collect()
        print(f"Unique chromosomes: {unique_chroms['chromosome'].to_list()}")
        
        # Positions should be positive integers
        min_pos: int = df.select(pl.col('position').min()).collect().item()
        max_pos: int = df.select(pl.col('position').max()).collect().item()
        print(f"Position range: {min_pos:,} - {max_pos:,}")
        
        assert min_pos > 0, "Positions should be positive"
        assert max_pos > min_pos, "Should have a range of positions"
    
    def test_download_multiple_chromosomes(self, temp_cache_dir: Optional[str]) -> None:
        """Test downloading multiple chromosomes."""
        # Download 21 and 22 (smaller chromosomes for faster test)
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            merge_parquets=False
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download files
        results: Dict[str, Dict[str, Path]] = downloader.download_all()
        
        # Should have both chromosomes
        assert 'chr21' in results
        assert 'chr22' in results
        
        # Check each chromosome has all file types
        for chrom in ['chr21', 'chr22']:
            assert 'vcf' in results[chrom]
            assert 'index' in results[chrom]
            assert 'parquet' in results[chrom]
            
            # Verify files exist and are non-empty
            vcf_path: Path = results[chrom]['vcf']
            parquet_path: Path = results[chrom]['parquet']
            
            assert vcf_path.exists()
            assert parquet_path.exists()
            assert vcf_path.stat().st_size > 0
            assert parquet_path.stat().st_size > 0
            
            print(f"{chrom}: VCF {vcf_path.stat().st_size:,} bytes, Parquet {parquet_path.stat().st_size:,} bytes")
    
    def test_merge_multiple_chromosomes(self, temp_cache_dir: Optional[str]) -> None:
        """Test downloading and merging multiple chromosomes into single parquet."""
        # Download and merge 21 and 22
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            merge_parquets=True
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download and merge files
        results: Dict[str, Dict[str, Path]] = downloader.download_all()
        
        # Should have individual chromosomes plus merged
        assert 'chr21' in results
        assert 'chr22' in results
        assert '_merged' in results
        assert 'parquet' in results['_merged']
        
        merged_path: Path = results['_merged']['parquet']
        assert merged_path.exists()
        assert merged_path.stat().st_size > 0
        
        # Read merged data
        merged_df: pl.LazyFrame = pl.scan_parquet(merged_path)
        
        # Should have data from both chromosomes
        unique_chroms = merged_df.select(pl.col('chromosome').unique().sort()).collect()
        chrom_list: List[str] = unique_chroms['chromosome'].to_list()
        print(f"Chromosomes in merged file: {chrom_list}")
        
        # Should contain both 21 and 22 (and possibly others depending on naming)
        assert any('21' in str(c) for c in chrom_list), "Should contain chromosome 21"
        assert any('22' in str(c) for c in chrom_list), "Should contain chromosome 22"
        
        # Count variants per chromosome
        variant_counts = merged_df.group_by('chromosome').len().sort('chromosome').collect()
        print(f"Variant counts by chromosome:\n{variant_counts}")
        
        total_variants: int = merged_df.select(pl.count()).collect().item()
        print(f"Total variants in merged file: {total_variants:,}")
        assert total_variants > 0
    
    def test_read_merged_parquet_method(self, temp_cache_dir: Optional[str]) -> None:
        """Test the read_merged_parquet convenience method."""
        # Download chromosomes 21 and 22
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            merge_parquets=False  # Don't pre-merge, let the method handle it
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download files
        results: Dict[str, Dict[str, Path]] = downloader.download_all()
        
        # Use the convenience method to read all data
        lazy_frame: pl.LazyFrame = downloader.read_merged_parquet()
        assert isinstance(lazy_frame, pl.LazyFrame)
        
        # Collect to DataFrame
        df: pl.DataFrame = downloader.read_merged_parquet().collect()
        assert isinstance(df, pl.DataFrame)
        
        # Should have data from both chromosomes
        unique_chroms = df.select(pl.col('chromosome').unique().sort())
        chrom_list: List[str] = unique_chroms['chromosome'].to_list()
        print(f"Chromosomes in combined data: {chrom_list}")
        
        total_variants: int = len(df)
        print(f"Total variants from read_merged_parquet: {total_variants:,}")
        assert total_variants > 0
    
    def test_default_downloads_all_chromosomes(self, temp_cache_dir: Optional[str]) -> None:
        """Test that default constructor downloads all available chromosomes."""
        # Create downloader with no chromosome specification (should default to all)
        downloader: EnsemblDownloader = EnsemblDownloader(
            convert_to_parquet=False,  # Skip parquet for faster test
            merge_parquets=False
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Check that all available chromosomes are targeted
        expected_chromosomes: set[str] = downloader._discover_available_chromosomes()
        actual_chromosomes: set[str] = set(downloader.vcf_urls.keys())
        
        # Convert chr1 -> 1 format for comparison
        actual_chrom_names: set[str] = {k.replace('chr', '') for k in actual_chromosomes}
        
        print(f"Expected chromosomes: {sorted(expected_chromosomes)}")
        print(f"Actual chromosome targets: {sorted(actual_chrom_names)}")
        
        # Should target all discovered chromosomes
        assert actual_chrom_names == expected_chromosomes, "Should download all available chromosomes by default"
        
        # Should include major chromosomes
        major_chromosomes = {'1', '2', '21', '22', 'X', 'Y', 'MT'}
        assert major_chromosomes.issubset(actual_chrom_names), "Should include all major chromosomes"
        
        print(f"Default behavior will download {len(actual_chromosomes)} chromosomes")


# Pytest markers for different test categories
@pytest.mark.slow
@pytest.mark.integration  
@pytest.mark.download
class TestEnsemblDownloaderSlowIntegration(TestEnsemblDownloaderFunctional):
    """Slow integration tests that require significant download time."""
    
    def test_download_large_chromosome(self, temp_cache_dir: Optional[str]) -> None:
        """Test downloading a larger chromosome (chromosome 1)."""
        # This test downloads ~1GB so mark it as slow
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['1'],
            convert_to_parquet=True
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download files
        results: Dict[str, Dict[str, Path]] = downloader.download_all()
        
        # Verify download
        assert 'chr1' in results
        vcf_path: Path = results['chr1']['vcf']
        parquet_path: Path = results['chr1']['parquet']
        
        assert vcf_path.exists()
        assert parquet_path.exists()
        
        # Chromosome 1 should be large
        vcf_size = vcf_path.stat().st_size
        parquet_size = parquet_path.stat().st_size
        
        print(f"Chromosome 1 VCF: {vcf_size:,} bytes")
        print(f"Chromosome 1 Parquet: {parquet_size:,} bytes")
        
        # Should be substantial files
        assert vcf_size > 100_000_000, "Chromosome 1 VCF should be > 100MB"
        assert parquet_size > 10_000_000, "Chromosome 1 parquet should be > 10MB"
        
        # Read and verify data
        df = pl.scan_parquet(parquet_path)
        variant_count = df.select(pl.count()).collect().item()
        print(f"Chromosome 1 variant count: {variant_count:,}")
        
        assert variant_count > 1_000_000, "Chromosome 1 should have >1M variants"
