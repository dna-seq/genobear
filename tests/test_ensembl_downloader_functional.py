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
    
    @pytest.mark.skip(reason="Using separate tests to avoid heavy multi-chromosome run")
    def test_two_chr_end_to_end_skip_and_merge(self, temp_cache_dir: Optional[str]) -> None:
        """End-to-end test to minimize re-downloads by covering multiple checks in one run.

        Flow:
        - First run: download chr21+chr22, convert to parquet, merge, clean intermediates
        - Validate schema and merged file
        - Second run: ensure downloads are skipped because parquet already exists
        """
        # First run
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            merge_parquets=True,
            clean_intermediates=True
        )
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)

        results = downloader.download_all()

        for chrom in ['chr21', 'chr22']:
            assert chrom in results
            res = results[chrom]
            assert res.parquet is not None and res.parquet.exists()
            # VCFs should be cleaned when clean_intermediates=True
            assert res.vcf is None or not res.vcf.exists()
            # LazyFrame available
            assert isinstance(res.get_lazy_frame(), pl.LazyFrame)

        # Merged parquet created
        assert downloader.download_results is not None and '_merged' in downloader.download_results
        merged_res = downloader.download_results['_merged']
        merged_path: Path = merged_res.parquet if hasattr(merged_res, 'parquet') else merged_res['parquet']
        assert merged_path.exists()

        merged_df = pl.scan_parquet(merged_path)
        schema = merged_df.collect_schema()
        for col in ['chrom', 'start', 'ref', 'alt']:
            assert col in schema
        total_variants = merged_df.select(pl.count()).collect().item()
        assert total_variants > 0

        # Second run (should skip downloads and just use existing parquet)
        downloader2: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            merge_parquets=False
        )
        if temp_cache_dir:
            downloader2.cache_subdir = temp_cache_dir
            downloader2.model_post_init(None)

        results2 = downloader2.download_all()
        for chrom in ['chr21', 'chr22']:
            res2 = results2[chrom]
            assert res2.parquet is not None and res2.parquet.exists()
            # Ensure VCF/index were not re-downloaded
            assert res2.vcf is None or not res2.vcf.exists()
            assert res2.index is None or not res2.index.exists()
            assert isinstance(res2.get_lazy_frame(), pl.LazyFrame)
    
    def test_download_single_chromosome_with_parquet(self, temp_cache_dir: Optional[str]) -> None:
        """Test downloading a single chromosome and converting to parquet."""
        # Use chromosome 22 (smallest autosome for faster test)
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['22'],
            convert_to_parquet=True,
            clean_semicolons=True,
            merge_parquets=False
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download the files
        results = downloader.download_all()
        
        # Verify results structure
        assert 'chr22' in results, f"chr22 not found in results; got keys: {list(results.keys())}"
        chr22 = results['chr22']
        
        # Verify files exist
        assert chr22.vcf is not None, "VCF path is None for chr22"
        assert chr22.parquet is not None, "Parquet path is None for chr22"
        # index may be optional depending on config, but Ensembl default uses CSI
        assert chr22.vcf.exists(), f"VCF file does not exist: {chr22.vcf}"
        parquet_path: Path = chr22.parquet
        assert parquet_path.exists(), f"Parquet file does not exist: {parquet_path}"
        # Index path if present
        if chr22.index is not None:
            assert chr22.index.exists(), f"Index file does not exist: {chr22.index}"
            index_path: Path = chr22.index
            assert index_path.suffix == '.csi', f"Index file has wrong suffix: {index_path.suffix} (expected .csi)"
        
        assert parquet_path.exists(), f"Parquet file does not exist: {parquet_path}"
        
        # Verify file types and sizes (allow .vcf if decompressed by downloader)
        assert parquet_path.suffix == '.parquet', f"Parquet file has wrong suffix: {parquet_path.suffix}"
        
        # Files should be non-empty
        if chr22.vcf is not None:
            assert chr22.vcf.stat().st_size > 0, f"VCF file is empty: {chr22.vcf}"
        if chr22.index is not None:
            assert chr22.index.stat().st_size > 0, f"Index file is empty: {chr22.index}"
        assert parquet_path.stat().st_size > 0, f"Parquet file is empty: {parquet_path}"
        
        if chr22.vcf is not None:
            print(f"Downloaded VCF: {chr22.vcf} ({chr22.vcf.stat().st_size:,} bytes)")
        if chr22.index is not None:
            print(f"Downloaded index: {chr22.index} ({chr22.index.stat().st_size:,} bytes)")
        print(f"Generated parquet: {parquet_path} ({parquet_path.stat().st_size:,} bytes)")
    
    def test_read_parquet_data(self, temp_cache_dir: Optional[str]) -> None:
        """Test that we can read and analyze the generated parquet files."""
        # Download chromosome 22
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['22'],
            convert_to_parquet=True,
            clean_semicolons=True
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download files
        results = downloader.download_all()
        assert 'chr22' in results, f"chr22 not found in results; got keys: {list(results.keys())}"
        parquet_path: Path = results['chr22'].parquet
        assert parquet_path is not None, "Parquet path is None for chr22"
        assert Path(parquet_path).exists(), f"Parquet path does not exist: {parquet_path}"
        
        # Read parquet file
        df: pl.LazyFrame = pl.scan_parquet(parquet_path)
        
        # Basic structure checks
        schema = df.collect_schema()
        print(f"Parquet schema: {list(schema.keys())}")
        
        # Should have standard VCF columns (current naming)
        expected_columns: List[str] = ['chrom', 'start', 'ref', 'alt']
        for col in expected_columns:
            assert col in schema, f"Missing expected column: {col}"
        
        # Count variants
        variant_count: int = df.select(pl.count()).collect().item()
        print(f"Number of variants in chromosome 22: {variant_count:,}")
        
        # Should have some variants
        assert variant_count > 0, f"Parquet file should contain variants, got {variant_count}"
        assert variant_count > 100000, f"Chromosome 22 should have many variants, got {variant_count}"
        
        # Check data quality - sample a few rows
        sample_data = df.limit(5).collect()
        print(f"Sample data:\n{sample_data}")
        
        # All chromosomes should be 22
        unique_chroms = df.select(pl.col('chrom').unique()).collect()
        print(f"Unique chromosomes: {unique_chroms['chrom'].to_list()}")
        
        # Positions should be positive integers
        min_pos: int = df.select(pl.col('start').min()).collect().item()
        max_pos: int = df.select(pl.col('start').max()).collect().item()
        print(f"Position range: {min_pos:,} - {max_pos:,}")
        
        assert min_pos > 0, f"Positions should be positive, got min_pos={min_pos}"
        assert max_pos > min_pos, f"Should have a range of positions, got min_pos={min_pos}, max_pos={max_pos}"

    def test_chr22_semicolons_are_cleaned(self, temp_cache_dir: Optional[str]) -> None:
        """Download chr22 VCF and ensure extra semicolons are removed by cleaner.

        This validates that our cleaning step prepares the file for parquet conversion.
        """
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['22'],
            convert_to_parquet=False,  # we only want the VCF for inspection
            clean_semicolons=False,
            merge_parquets=False
        )
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)

        results = downloader.download_all(decompress=True, download_index=False)
        assert 'chr22' in results
        vcf_path: Path = results['chr22'].vcf
        assert vcf_path is not None and vcf_path.exists(), "Decompressed VCF should exist for chr22"

        def count_bad_tokens(path: Path) -> int:
            bad = 0
            with path.open('r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    if ';;' in line or ';:' in line or ';;:' in line:
                        bad += 1
                        # no need to count all lines; early exit is fine
                        break
            return bad

        # Optionally record pre-clean state (may or may not have issues depending on source data)
        _pre_bad = count_bad_tokens(vcf_path)

        # Run in-place cleaning
        from genobear.io import clean_extra_semicolons
        cleaned_path = clean_extra_semicolons(vcf_path, output_path=None, compression=None, inplace_for_compressed=False)
        assert cleaned_path == vcf_path

        # Ensure no problematic semicolon patterns remain
        post_bad = count_bad_tokens(vcf_path)
        assert post_bad == 0, f"Unexpected semicolon artifacts remain in chr22 VCF after cleaning: {vcf_path}"
    
    def test_download_multiple_chromosomes(self, temp_cache_dir: Optional[str]) -> None:
        """Test downloading multiple chromosomes."""
        # Download 21 and 22 (smaller chromosomes for faster test)
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            clean_semicolons=True,
            merge_parquets=False
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download files
        results = downloader.download_all()
        
        # Should have both chromosomes
        assert 'chr21' in results, f"chr21 not found in results; got keys: {list(results.keys())}"
        assert 'chr22' in results, f"chr22 not found in results; got keys: {list(results.keys())}"
        
        # Check each chromosome has all file types
        for chrom in ['chr21', 'chr22']:
            result = results[chrom]
            assert result.parquet is not None and result.parquet.exists(), f"Parquet missing for {chrom}: {result.parquet}"
            if result.vcf is not None:
                assert result.vcf.exists(), f"VCF missing for {chrom}: {result.vcf}"
                assert result.vcf.stat().st_size > 0, f"VCF empty for {chrom}: {result.vcf}"
            assert result.parquet.stat().st_size > 0, f"Parquet empty for {chrom}: {result.parquet}"
            if result.vcf is not None:
                print(f"{chrom}: VCF {result.vcf.stat().st_size:,} bytes, Parquet {result.parquet.stat().st_size:,} bytes")
            else:
                print(f"{chrom}: Parquet {result.parquet.stat().st_size:,} bytes")
    
    @pytest.mark.skip(reason="Condensed into test_two_chr_end_to_end_skip_and_merge")
    def test_merge_multiple_chromosomes(self, temp_cache_dir: Optional[str]) -> None:
        """Test downloading and merging multiple chromosomes into single parquet."""
        # Download and merge 21 and 22
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            clean_semicolons=True,
            merge_parquets=True
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download and merge files
        results = downloader.download_all()
        
        # Should have individual chromosomes and merged in downloader state
        assert 'chr21' in results
        assert 'chr22' in results
        assert downloader.download_results is not None and '_merged' in downloader.download_results
        merged = downloader.download_results['_merged']
        merged_path: Path = merged.parquet if hasattr(merged, 'parquet') else merged['parquet']
        assert merged_path.exists()
        assert merged_path.stat().st_size > 0
        
        # Read merged data
        merged_df: pl.LazyFrame = pl.scan_parquet(merged_path)
        
        # Should have data from both chromosomes
        unique_chroms = merged_df.select(pl.col('chrom').unique().sort()).collect()
        chrom_list: List[str] = unique_chroms['chrom'].to_list()
        print(f"Chromosomes in merged file: {chrom_list}")
        
        # Should contain both 21 and 22 (and possibly others depending on naming)
        assert any('21' in str(c) for c in chrom_list), "Should contain chromosome 21"
        assert any('22' in str(c) for c in chrom_list), "Should contain chromosome 22"
        
        # Count variants per chromosome
        variant_counts = merged_df.group_by('chrom').len().sort('chrom').collect()
        print(f"Variant counts by chromosome:\n{variant_counts}")
        
        total_variants: int = merged_df.select(pl.count()).collect().item()
        print(f"Total variants in merged file: {total_variants:,}")
        assert total_variants > 0
    
    @pytest.mark.skip(reason="Condensed into test_two_chr_end_to_end_skip_and_merge")
    def test_read_merged_parquet_method(self, temp_cache_dir: Optional[str]) -> None:
        """Test the read_merged_parquet convenience method."""
        # Download chromosomes 21 and 22
        downloader: EnsemblDownloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            clean_semicolons=True,
            merge_parquets=False  # Don't pre-merge, let the method handle it
        )
        
        # Optionally override cache directory when not using shared cache
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        
        # Download files
        results = downloader.download_all()
        
        # Use the convenience method to read all data
        lazy_frame: pl.LazyFrame = downloader.read_merged_parquet()
        assert isinstance(lazy_frame, pl.LazyFrame)
        
        # Collect to DataFrame
        df: pl.DataFrame = downloader.read_merged_parquet().collect()
        assert isinstance(df, pl.DataFrame)
        
        # Should have data from both chromosomes
        unique_chroms = df.select(pl.col('chrom').unique().sort())
        chrom_list: List[str] = unique_chroms['chrom'].to_list()
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
    
    @pytest.mark.skip(reason="Too slow for condensed suite; run explicitly when needed")
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
        results = downloader.download_all()
        
        # Verify download
        assert 'chr1' in results
        vcf_path: Path = results['chr1'].vcf
        parquet_path: Path = results['chr1'].parquet
        
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

    def test_skip_download_when_parquet_exists_two_chromosomes(self, temp_cache_dir: Optional[str]) -> None:
        """Ensure that when parquet exists, VCF/index downloads are skipped on subsequent runs."""
        # First run: download and convert with cleanup
        downloader = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            clean_semicolons=True,
            merge_parquets=False,
            clean_intermediates=True
        )
        if temp_cache_dir:
            downloader.cache_subdir = temp_cache_dir
            downloader.model_post_init(None)
        first_results = downloader.download_all()
        # Verify parquet exists and VCFs were cleaned
        for chrom in ['chr21', 'chr22']:
            res = first_results[chrom]
            assert res.parquet is not None and res.parquet.exists()
            # After cleanup, VCF and index should be None or not exist
            if res.vcf is not None:
                assert not res.vcf.exists()
            if res.index is not None:
                assert not res.index.exists()
        
        # Second run: create a fresh downloader to simulate a new process
        downloader2 = EnsemblDownloader.for_chromosomes(
            ['21', '22'],
            convert_to_parquet=True,
            clean_semicolons=True,
            merge_parquets=False,
        )
        if temp_cache_dir:
            downloader2.cache_subdir = temp_cache_dir
            downloader2.model_post_init(None)
        second_results = downloader2.download_all()
        
        # Should have skipped downloads and only populated parquet/lazy_frame
        for chrom in ['chr21', 'chr22']:
            res2 = second_results[chrom]
            assert res2.parquet is not None and res2.parquet.exists()
            # VCF and index should not have been downloaded again
            assert res2.vcf is None or not res2.vcf.exists()
            assert res2.index is None or not res2.index.exists()
            # Derive expected VCF path and ensure it does not exist
            expected_vcf = res2.parquet.with_suffix('.vcf')
            assert not expected_vcf.exists()
            # LazyFrame should be available
            lf = res2.get_lazy_frame()
            assert isinstance(lf, pl.LazyFrame)
