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
from typing import Generator, Iterator
from eliot import start_action

from genobear.downloaders.vcf_downloader import ClinVarDownloader, download_clinvar_multiple_assemblies
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
            cache_subdir=str(cache_dir)
        )
        # Pre-download the files once for the session
        downloader.fetch_files()
        return downloader

    @pytest.mark.integration
    def test_read_first_10_lines_from_vcf(self, cached_clinvar_downloader: ClinVarDownloader) -> pl.DataFrame:
        """Test reading first 10 lines directly from VCF with optimized settings."""
        with start_action(action_type="test_read_first_10_lines"):
            # Use cached downloader - files already downloaded
            print(f"Using cached VCF file: {cached_clinvar_downloader.vcf_path}")
            
            # Configure Polars for handling large strings and reduce chunk size
            with pl.Config(streaming_chunk_size=50):  # Very small chunks for large strings
                # Read with minimal INFO fields to avoid string length issues
                df_lazy = read_vcf_file(
                    cached_clinvar_downloader.vcf_path,
                    info_fields=[],  # Skip INFO fields to avoid long strings
                    streaming=False,  # Disable streaming since it's not implemented in polars-bio
                    save_parquet=None,  # Don't save to parquet
                    return_lazy=True
                )
                
                # Take first 10 lines and collect to DataFrame
                first_10_lines = df_lazy.head(10).collect()
            
            # Verify we got exactly 10 lines (or fewer if file is smaller)
            assert len(first_10_lines) <= 10
            assert len(first_10_lines) > 0
            
            # Verify structure
            assert isinstance(first_10_lines, pl.DataFrame)
            
            # Print first 10 lines for inspection
            print("\n" + "="*80)
            print("FIRST 10 LINES OF CLINVAR VCF FILE:")
            print("="*80)
            print(first_10_lines)
            print("="*80)
            
            # Verify some expected columns (note: polars-bio uses 'start' instead of 'pos')
            expected_columns = ['chrom', 'start', 'ref', 'alt']
            for col in expected_columns:
                assert col in first_10_lines.columns, f"Missing column: {col}"
            
            # Verify data types are reasonable
            assert first_10_lines['start'].dtype in [pl.Int32, pl.Int64, pl.UInt32, pl.UInt64]
            assert first_10_lines['chrom'].dtype == pl.String
            
            return first_10_lines

    @pytest.mark.integration
    def test_parquet_conversion_cached(self, cached_clinvar_downloader: ClinVarDownloader, temp_cache_dir: Path) -> pl.DataFrame:
        """Test converting first 10 lines to parquet using cached downloader."""
        with start_action(action_type="test_parquet_conversion_cached"):
            print(f"Using cached VCF file: {cached_clinvar_downloader.vcf_path}")
            
            # Configure Polars for handling large strings
            with pl.Config(streaming_chunk_size=50):
                # Read just first 10 lines with minimal INFO fields
                df_lazy = read_vcf_file(
                    cached_clinvar_downloader.vcf_path,
                    info_fields=[],  # Skip problematic INFO fields
                    streaming=False,
                    save_parquet=None,
                    return_lazy=True
                ).head(10)  # Limit to first 10 lines
                
                # Convert to DataFrame first
                first_10_lines = df_lazy.collect()
                
                # Now save to parquet
                parquet_path = temp_cache_dir / "clinvar_first_10.parquet"
                first_10_lines.write_parquet(str(parquet_path))
            
            # Verify parquet file was created
            assert parquet_path.exists()
            assert parquet_path.stat().st_size > 100  # At least 100 bytes
            
            # Read back from parquet
            parquet_df = pl.read_parquet(parquet_path)
            
            # Verify we got the same data
            assert len(parquet_df) == len(first_10_lines)
            assert parquet_df.columns == first_10_lines.columns
            
            # Print first 10 lines for inspection
            print("\n" + "="*80)
            print("FIRST 10 LINES FROM PARQUET FILE:")
            print("="*80)
            print(parquet_df)
            print("="*80)
            
            return parquet_df

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
        
        # Should raise an error when trying to fetch
        with pytest.raises(Exception):  # Could be various network errors
            downloader.fetch_files()


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
            cache_subdir=str(temp_path / "test_clinvar")
        )
        
        print("Testing ClinVar downloader...")
        result = test_instance.test_read_first_10_lines_from_vcf(downloader)
        print("✅ Test completed successfully!")
        print(f"Read {len(result)} lines from ClinVar VCF")
        
        print(f"Check logs in {logs_dir} for detailed information:")
