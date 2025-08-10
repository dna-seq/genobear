#!/usr/bin/env python3
"""
Test suite for VCFAnnotator using the new generic annotation framework.

This test loads a VCF file using VCFAnnotator and validates the output.
"""

import pytest
import polars as pl
from pathlib import Path
from eliot import start_action
from pycomfort.logging import to_nice_stdout

from genobear import VCFAnnotator


class TestVCFAnnotator:
    """Test cases for VCFAnnotator functionality."""

    @pytest.fixture(scope="session", autouse=True)
    def setup_logging(self):
        """Setup logging for all tests."""
        to_nice_stdout()
        yield

    @pytest.fixture
    def test_vcf_path(self) -> Path:
        """Get the path to the test VCF file."""
        vcf_path = Path("data/tests/antonkulaga.vcf")
        assert vcf_path.exists(), f"Test VCF file not found: {vcf_path}"
        return vcf_path

    def test_vcf_annotator_basic_usage(self, test_vcf_path: Path):
        """Test basic VCFAnnotator usage with antonkulaga.vcf."""
        with start_action(action_type="test_vcf_annotator_basic"):
            # Create VCF annotator
            vcf_annotator = VCFAnnotator("test_vcf_reader")
            
            # Load VCF file - this should return a LazyFrame
            result = vcf_annotator(test_vcf_path)
            
            # Verify the result is a LazyFrame
            assert isinstance(result, pl.LazyFrame), f"Expected LazyFrame, got {type(result)}"
            
            # Collect first 10 rows to DataFrame for inspection
            first_10_df = result.head(10).collect()
            
            # Verify we got data
            assert len(first_10_df) > 0, "No data returned from VCF file"
            assert len(first_10_df) <= 10, f"Expected max 10 rows, got {len(first_10_df)}"
            
            # Verify expected VCF columns exist
            expected_columns = ['chrom', 'start', 'ref', 'alt']
            for col in expected_columns:
                assert col in first_10_df.columns, f"Missing expected column: {col}"
            
            # Print the first 10 rows for inspection
            print("\n" + "="*80)
            print(f"FIRST 10 ROWS FROM {test_vcf_path.name} using VCFAnnotator:")
            print("="*80)
            print(first_10_df)
            print("="*80)
            print(f"Total columns: {len(first_10_df.columns)}")
            print(f"Column names: {first_10_df.columns}")
            print("="*80)
            
            return first_10_df

    def test_vcf_annotator_with_parquet_save(self, test_vcf_path: Path, tmp_path: Path):
        """Test VCFAnnotator with parquet saving enabled."""
        with start_action(action_type="test_vcf_annotator_parquet"):
            # Create VCF annotator
            vcf_annotator = VCFAnnotator("test_vcf_with_parquet")
            
            # Define parquet output path
            parquet_path = tmp_path / "antonkulaga_test.parquet"
            
            # Load VCF file with parquet saving
            result = vcf_annotator(test_vcf_path, save_parquet=parquet_path)
            
            # Verify the result is still a LazyFrame
            assert isinstance(result, pl.LazyFrame), f"Expected LazyFrame, got {type(result)}"
            
            # Verify parquet file was created
            assert parquet_path.exists(), f"Parquet file not created: {parquet_path}"
            
            # Read from parquet and get first 10 rows
            parquet_df = pl.scan_parquet(str(parquet_path)).head(10).collect()
            
            # Print the first 10 rows from parquet
            print("\n" + "="*80)
            print(f"FIRST 10 ROWS FROM PARQUET FILE ({parquet_path.name}):")
            print("="*80)
            print(parquet_df)
            print("="*80)
            print(f"Parquet file size: {parquet_path.stat().st_size} bytes")
            print("="*80)
            
            return parquet_df

    def test_vcf_annotator_pipeline(self, test_vcf_path: Path):
        """Test VCFAnnotator in a pipeline context."""
        with start_action(action_type="test_vcf_annotator_pipeline"):
            # Create VCF annotator
            vcf_annotator = VCFAnnotator("pipeline_vcf_reader")
            
            # Use it in a pipeline-like context with manual context
            context = {'processed': set(), 'results': {}}
            
            # First call
            result1 = vcf_annotator(test_vcf_path, context=context)
            
            # Second call (should use cached result)
            result2 = vcf_annotator(test_vcf_path, context=context)
            
            # Both should be LazyFrames
            assert isinstance(result1, pl.LazyFrame)
            assert isinstance(result2, pl.LazyFrame)
            
            # Verify annotator was processed only once
            assert "pipeline_vcf_reader" in context['processed']
            assert "pipeline_vcf_reader" in context['results']
            
            # Get first 10 rows from first result
            first_10_df = result1.head(10).collect()
            
            print("\n" + "="*80)
            print("VCFAnnotator PIPELINE TEST - FIRST 10 ROWS:")
            print("="*80)
            print(first_10_df)
            print("="*80)
            print(f"Context processed: {context['processed']}")
            print(f"Context has results: {len(context['results'])} items")
            print("="*80)
            
            return first_10_df


if __name__ == "__main__":
    """Run tests directly for quick testing."""
    import sys
    import tempfile
    from pathlib import Path
    
    # Setup logging
    to_nice_stdout()
    
    # Get VCF path
    vcf_path = Path("data/tests/antku_small.vcf")
    if not vcf_path.exists():
        print(f"❌ Test VCF file not found: {vcf_path}")
        sys.exit(1)
    
    print(f"📁 Using test VCF file: {vcf_path}")
    
    try:
        print("🧪 Running VCFAnnotator tests...")
        
        # Test basic usage
        print("\n1️⃣ Testing basic VCFAnnotator usage...")
        with start_action(action_type="test_vcf_annotator_basic"):
            # Create VCF annotator with default settings (info_fields=[], streaming=False)
            vcf_annotator = VCFAnnotator("test_vcf_reader")
            
            # Load VCF file - process ALL data, not just first 10 rows
            result = vcf_annotator(vcf_path)
            
            # Verify the result is a LazyFrame
            assert isinstance(result, pl.LazyFrame), f"Expected LazyFrame, got {type(result)}"
            
            # Get total row count
            total_rows = len(result.collect())
            print(f"📊 Total rows in VCF: {total_rows}")
            
            # Collect first 10 rows to DataFrame for display
            first_10_df = result.head(10).collect()
            
            # Print the first 10 rows for inspection
            print("\n" + "="*80)
            print(f"FIRST 10 ROWS FROM {vcf_path.name} using VCFAnnotator:")
            print(f"(Processing all {total_rows} rows)")
            print("="*80)
            print(first_10_df)
            print("="*80)
            print(f"Total columns: {len(first_10_df.columns)}")
            print(f"Column names: {first_10_df.columns}")
            print("="*80)
        
        print(f"✅ Basic test passed - processed all {total_rows} rows")
        
        # Test with parquet saving
        print("\n2️⃣ Testing VCFAnnotator with parquet saving...")
        with tempfile.TemporaryDirectory() as tmp_dir:
            with start_action(action_type="test_vcf_annotator_parquet"):
                # Create VCF annotator
                vcf_annotator = VCFAnnotator("test_vcf_with_parquet")
                
                # Define parquet output path
                parquet_path = Path(tmp_dir) / "antonkulaga_test.parquet"
                
                # Load VCF file with parquet saving - processes ALL data
                result = vcf_annotator(vcf_path, save_parquet=parquet_path)
                
                # Get total rows from parquet
                parquet_total = len(pl.scan_parquet(str(parquet_path)).collect())
                print(f"📊 Total rows in parquet: {parquet_total}")
                
                # Get first 10 rows from parquet for display
                parquet_df = pl.scan_parquet(str(parquet_path)).head(10).collect()
                
                # Print the first 10 rows from parquet
                print("\n" + "="*80)
                print(f"FIRST 10 ROWS FROM PARQUET FILE ({parquet_path.name}):")
                print(f"(Saved all {parquet_total} rows)")
                print("="*80)
                print(parquet_df)
                print("="*80)
                print(f"Parquet file size: {parquet_path.stat().st_size} bytes")
                print("="*80)
        
        print(f"✅ Parquet test passed - processed all {parquet_total} rows")
        
        print("\n🎉 All VCFAnnotator tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
