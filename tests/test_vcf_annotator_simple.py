#!/usr/bin/env python3
"""
Simple test demonstrating VCFAnnotator usage with antku_small.vcf.
"""

import sys
from pathlib import Path
from eliot import start_action
from pycomfort.logging import to_nice_stdout
from genobear import VCFAnnotator


def main():
    """Test VCFAnnotator with antku_small.vcf file."""
    # Setup logging
    to_nice_stdout()
    
    # Get VCF path
    vcf_path = Path("data/tests/antku_small.vcf")
    if not vcf_path.exists():
        print(f"❌ Test VCF file not found: {vcf_path}")
        return 1
    
    print(f"📁 Using test VCF file: {vcf_path}")
    
    try:
        print("🧪 Testing VCFAnnotator...")
        
        with start_action(action_type="test_vcf_annotator"):
            # Create VCF annotator with default settings (info_fields=[], streaming=False)
            print("🔧 Creating VCFAnnotator with default settings...")
            vcf_annotator = VCFAnnotator("vcf_reader")
            
            # Load VCF file - process ALL data
            print("📖 Loading VCF file...")
            result = vcf_annotator(vcf_path)
            
            # Get total row count
            total_rows = len(result.collect())
            print(f"📊 Total rows processed: {total_rows}")
            
            # Get first 10 rows for display
            first_10_df = result.head(10).collect()
            
            # Print the first 10 rows
            print("\n" + "="*80)
            print(f"FIRST 10 ROWS FROM {vcf_path.name} using VCFAnnotator:")
            print(f"(Successfully processed all {total_rows} rows)")
            print("="*80)
            print(first_10_df)
            print("="*80)
            print(f"Total columns: {len(first_10_df.columns)}")
            print(f"Column names: {first_10_df.columns}")
            print("="*80)
            
            # Show data types
            print("🔍 Column data types:")
            for col, dtype in zip(first_10_df.columns, first_10_df.dtypes):
                print(f"  {col}: {dtype}")
            
            # Show some statistics
            print(f"\n📈 Quick stats:")
            print(f"  • Chromosomes: {first_10_df['chrom'].unique().to_list()}")
            print(f"  • Position range: {first_10_df['start'].min()} - {first_10_df['start'].max()}")
            print(f"  • Reference alleles: {first_10_df['ref'].unique().to_list()}")
            print(f"  • Filters: {first_10_df['filter'].unique().to_list()}")
        
        print(f"\n🎉 VCFAnnotator test successful!")
        print(f"✅ Processed {total_rows} variants from {vcf_path.name}")
        print(f"✅ Returned polars LazyFrame with {len(first_10_df.columns)} columns")
        print(f"✅ Default settings: info_fields=[], streaming=False")
        
        return 0
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
