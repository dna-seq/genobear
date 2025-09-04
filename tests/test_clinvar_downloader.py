#!/usr/bin/env python3
"""
Test suite for ClinVarDownloader using the new pooch.retrieve() approach.

This test downloads ClinVar data, converts it to parquet, and validates
the first 10 lines can be read correctly.
"""

import pytest
import polars as pl
from pathlib import Path
import tempfile
import shutil
import os
from typing import Generator
from eliot import start_action

from genobear.downloaders.clinvar_downloader import ClinVarDownloader
from genobear.io import read_vcf_file
from pycomfort.logging import to_nice_stdout, to_nice_file

class TestClinVarDownloader:
    """Test cases for ClinVarDownloader functionality."""

    @pytest.fixture(scope="session", autouse=True)
    def setup_logging(self) -> Generator[None, None, None]:
        """Setup logging for all tests using pycomfort."""
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        json_log_path = logs_dir / "test_clinvar.json"
        rendered_log_path = logs_dir / "test_clinvar.log"
        
        # Setup nice logging to both files and stdout
        to_nice_file(json_log_path, rendered_log_path)
        to_nice_stdout()
        
        yield
        
        # Log files will remain for inspection

    @pytest.fixture
    def temp_cache_dir(self) -> Generator[Path, None, None]:
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp(prefix="genobear_test_")
        yield Path(temp_dir)
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture(scope="session")
    def cached_clinvar_downloader(self) -> ClinVarDownloader:
        """Create a session-scoped ClinVarDownloader that reuses downloads across tests."""
        # Use a persistent cache directory that won't be cleaned up
        cache_dir = Path.home() / ".cache" / "genobear_test" / "clinvar_cache"
        downloader = ClinVarDownloader(
            assembly='GRCh38',
            subdir_name=str(cache_dir)
        )
        # Pre-download the files once for the session
        downloader.download_all()
        return downloader

    @pytest.mark.integration
    def test_read_first_10_lines_from_parquet(self, cached_clinvar_downloader: ClinVarDownloader) -> pl.DataFrame:
        """Test reading first 10 lines from parquet file with optimized settings."""
        with start_action(action_type="test_read_first_10_lines_from_parquet"):
            # Use cached downloader - files already downloaded and converted to parquet
            # Get the download result which should have parquet
            result = cached_clinvar_downloader.download_results['clinvar']
            
            # MultiVCFDownloader prioritizes parquet files when available
            if result.has_parquet:
                print(f"Using cached parquet file: {result.parquet}")
                # Use the lazy frame from the downloader for efficiency
                df_lazy = result.get_lazy_frame()
            elif result.has_vcf:
                print(f"Using cached VCF file: {result.vcf}")
                # Fallback to VCF if parquet not available
                df_lazy = read_vcf_file(
                    result.vcf,
                    info_fields=[],  # Skip INFO fields to avoid long strings
                    save_parquet=None  # Don't save to parquet
                )
            else:
                raise ValueError("No VCF or parquet file available in download results")
            
            # Take first 10 lines - handle both LazyFrame and DataFrame
            if isinstance(df_lazy, pl.LazyFrame):
                first_10_lines = df_lazy.head(10).collect()
            else:
                # Already a DataFrame
                first_10_lines = df_lazy.head(10)
            
            # Verify we got exactly 10 lines (or fewer if file is smaller)
            assert len(first_10_lines) <= 10, f"Expected ≤10 lines, got {len(first_10_lines)}"
            assert len(first_10_lines) > 0, "Expected at least 1 line of data"
            
            # Verify structure
            assert isinstance(first_10_lines, pl.DataFrame), f"Expected DataFrame, got {type(first_10_lines)}"
            
            # Print first 10 lines for inspection
            print("\n" + "="*80)
            print("FIRST 10 LINES OF CLINVAR DATA:")
            print("="*80)
            print(first_10_lines)
            print("="*80)
            
            # Verify some expected columns (note: polars-bio uses 'start' instead of 'pos')
            expected_columns = ['chrom', 'start', 'ref', 'alt']
            for col in expected_columns:
                assert col in first_10_lines.columns, f"Missing column: {col}"
            
            # Verify data types are reasonable
            assert first_10_lines['start'].dtype in [pl.Int32, pl.Int64, pl.UInt32, pl.UInt64], f"Unexpected start column type: {first_10_lines['start'].dtype}"
            assert first_10_lines['chrom'].dtype == pl.String, f"Unexpected chrom column type: {first_10_lines['chrom'].dtype}"
            
            return first_10_lines

    @pytest.mark.integration
    def test_parquet_functionality(self, cached_clinvar_downloader: ClinVarDownloader, temp_cache_dir: Path) -> pl.DataFrame:
        """Test that parquet conversion functionality works correctly."""
        with start_action(action_type="test_parquet_functionality"):
            # Get the download result which should have parquet
            result = cached_clinvar_downloader.download_results['clinvar']
            
            # MultiVCFDownloader should have automatically converted to parquet
            assert result.has_parquet, "Expected parquet file to be available"
            print(f"Using automatically created parquet file: {result.parquet}")
            
            # Test that we can read from the parquet efficiently
            df_lazy = result.get_lazy_frame()
            
            # Take first 10 lines
            first_10_lines = df_lazy.head(10).collect()
            
            # Now save a copy to our temp directory to test round-trip
            parquet_path = temp_cache_dir / "clinvar_first_10_copy.parquet"
            first_10_lines.write_parquet(str(parquet_path))
            
            # Verify parquet file was created
            assert parquet_path.exists(), f"Parquet file was not created at {parquet_path}"
            assert parquet_path.stat().st_size > 100, f"Parquet file is too small: {parquet_path.stat().st_size} bytes"
            
            # Read back from the copied parquet
            parquet_df = pl.read_parquet(parquet_path)
            
            # Verify we got the same data
            assert len(parquet_df) == len(first_10_lines), f"Length mismatch: original={len(first_10_lines)}, reloaded={len(parquet_df)}"
            assert parquet_df.columns == first_10_lines.columns, f"Columns mismatch: original={first_10_lines.columns}, reloaded={parquet_df.columns}"
            
            # Print first 10 lines for inspection
            print("\n" + "="*80)
            print("FIRST 10 LINES FROM PARQUET FILE:")
            print("="*80)
            print(parquet_df)
            print("="*80)
            
            return parquet_df

    @pytest.mark.integration
    def test_multi_vcf_downloader_features(self, cached_clinvar_downloader: ClinVarDownloader) -> None:
        """Test that ClinVarDownloader properly inherits MultiVCFDownloader features."""
        with start_action(action_type="test_multi_vcf_downloader_features"):
            # Verify it has MultiVCFDownloader features
            assert hasattr(cached_clinvar_downloader, 'download_results'), "Should have download_results attribute"
            assert hasattr(cached_clinvar_downloader, 'lazy_frames'), "Should have lazy_frames property"
            assert hasattr(cached_clinvar_downloader, 'parquet_paths'), "Should have parquet_paths property"
            assert hasattr(cached_clinvar_downloader, 'split_all_variants'), "Should have split_all_variants method"
            
            # Test that download_results contains expected structure
            assert 'clinvar' in cached_clinvar_downloader.download_results, "Should have 'clinvar' key in download_results"
            result = cached_clinvar_downloader.download_results['clinvar']
            
            # Test parquet functionality
            assert result.has_parquet, "Should have parquet file available"
            parquet_paths = cached_clinvar_downloader.parquet_paths
            assert 'clinvar' in parquet_paths, "Should have 'clinvar' in parquet_paths"
            assert parquet_paths['clinvar'].exists(), f"Parquet file should exist: {parquet_paths['clinvar']}"
            
            # Test lazy frames functionality
            lazy_frames = cached_clinvar_downloader.lazy_frames
            assert 'clinvar' in lazy_frames, "Should have 'clinvar' in lazy_frames"
            assert isinstance(lazy_frames['clinvar'], pl.LazyFrame), "Should be a LazyFrame"
            
            # Test that we can collect a small sample
            sample = lazy_frames['clinvar'].head(5).collect()
            assert len(sample) <= 5, f"Expected ≤5 rows, got {len(sample)}"
            assert len(sample) > 0, "Expected at least 1 row"
            
            print(f"✅ MultiVCFDownloader features working correctly")
            print(f"   - Parquet file: {result.parquet}")
            print(f"   - Lazy frame shape: {sample.shape}")
            print(f"   - Available features: download_results, lazy_frames, parquet_paths, split_all_variants")

class TestClinVarDownloaderErrors:
    """Test error conditions and edge cases."""

    def test_invalid_assembly(self) -> None:
        """Test that invalid assembly raises appropriate error."""
        with pytest.raises(ValueError):
            ClinVarDownloader(assembly='invalid_assembly')

    @pytest.mark.integration
    def test_download_with_invalid_url(self) -> None:
        """Test download with invalid base URL."""
        downloader = ClinVarDownloader(
            assembly='GRCh38',
            base_url="https://invalid.example.com/nonexistent/"
        )
        
        # The downloader should handle network errors gracefully
        # and not raise exceptions, but the download should fail
        try:
            downloader.download_all()
            # If no exception is raised, check that the download results indicate failure
            if downloader.download_results is not None and 'clinvar' in downloader.download_results:
                result = downloader.download_results['clinvar']
                if result.vcf is not None:
                    assert not result.vcf.exists(), f"VCF file should not exist after failed download: {result.vcf}"
            # If download_results is None or empty, that's also acceptable for failed downloads
        except Exception as e:
            # If an exception is raised, that's also acceptable
            # but we should log it for debugging
            print(f"Exception raised during failed download (acceptable): {e}")
            assert True  # This is fine


# Helper for running tests manually
if __name__ == "__main__":
    """Run tests manually for development."""
    import sys
    
    # Setup logging using pycomfort
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    json_log_path = logs_dir / "manual_test_clinvar.json"
    rendered_log_path = logs_dir / "manual_test_clinvar.log"
    
    to_nice_file(json_log_path, rendered_log_path)
    to_nice_stdout()
    
    # Create test instance
    test_instance = TestClinVarDownloader()
    
    # Run a quick test
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        downloader = ClinVarDownloader(
            assembly='GRCh38',
            subdir_name=str(temp_path / "test_clinvar")
        )
        
        print("Testing ClinVar downloader...")
        # Download files first
        downloader.download_all()
        result = test_instance.test_read_first_10_lines_from_parquet(downloader)
        print("✅ Test completed successfully!")
        print(f"Read {len(result)} lines from ClinVar data")
        
        print(f"Check logs in {logs_dir} for detailed information:")
