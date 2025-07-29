#!/usr/bin/env python3
"""
Demonstration script showing the enhanced pathogenicity analysis in genobear annotations.

This addresses the user's frustration about missing pathogenicity validation by showing
exactly what clinical significance information is now captured and reported.
"""

import asyncio
from pathlib import Path
import polars as pl
from eliot import start_action

import genobear as gb
from genobear.annotation.annotate import extract_pathogenicity_stats, report_pathogenicity_summary


def demo_pathogenicity_analysis():
    """
    Demonstrate the enhanced pathogenicity analysis capabilities.
    """
    with start_action(action_type="demo_pathogenicity_analysis") as action:
        print("🧬 GenoBear Pathogenicity Analysis Demo")
        print("="*50)
        
        # 1. Download ClinVar database with pathogenicity info
        print("📥 Step 1: Downloading ClinVar database...")
        
        output_folder = Path("demo_data")
        result = gb.download_clinvar_sync(
            assemblies=["hg38"],
            output_folder=output_folder,
            convert_to_parquet_files=True,
            force=False
        )
        
        # Find the ClinVar parquet file
        clinvar_parquet = None
        for file_path, download_result in result.items():
            if download_result and Path(file_path).suffix == '.parquet':
                clinvar_parquet = Path(file_path)
                break
        
        if not clinvar_parquet:
            print("❌ Failed to download ClinVar database")
            return
        
        print(f"✅ Downloaded ClinVar database: {clinvar_parquet}")
        
        # 2. Analyze pathogenicity in the source database
        print("\n📊 Step 2: Analyzing pathogenicity in ClinVar database...")
        
        df = pl.read_parquet(clinvar_parquet)
        stats = extract_pathogenicity_stats(df)
        
        print(f"Total ClinVar variants: {len(df):,}")
        print("\nPathogenicity Breakdown:")
        print(f"🔴 Pathogenic: {stats['pathogenic']:,}")
        print(f"🟠 Likely Pathogenic: {stats['likely_pathogenic']:,}")
        print(f"🟡 Uncertain Significance: {stats['uncertain_significance']:,}")
        print(f"🟢 Likely Benign: {stats['likely_benign']:,}")
        print(f"🟢 Benign: {stats['benign']:,}")
        print(f"💊 Drug Response: {stats['drug_response']:,}")
        print(f"⚠️  Risk Factor: {stats['risk_factor']:,}")
        print(f"🛡️  Protective: {stats['protective']:,}")
        print(f"📊 Association: {stats['association']:,}")
        print(f"🔧 Other: {stats['other']:,}")
        
        # Calculate key metrics
        pathogenic_total = stats['pathogenic'] + stats['likely_pathogenic']
        benign_total = stats['benign'] + stats['likely_benign']
        clinical_total = pathogenic_total + benign_total + stats['uncertain_significance']
        
        print(f"\n📈 Summary Statistics:")
        print(f"Disease-causing variants: {pathogenic_total:,}")
        print(f"Benign variants: {benign_total:,}")
        print(f"VUS variants: {stats['uncertain_significance']:,}")
        print(f"Clinical significance coverage: {(clinical_total/len(df)*100):.1f}%")
        
        # 3. Test annotation with a sample VCF
        print("\n🔧 Step 3: Testing annotation workflow...")
        
        # Check if we have a test VCF file
        test_vcf = Path("tests/data/longevity_snps_1000genom.vcf.gz")
        if test_vcf.exists():
            print(f"Using test VCF: {test_vcf}")
            
            # Annotate with the enhanced system
            output_path = output_folder / "annotated_demo.parquet"
            result = gb.annotate_vcf(
                vcf_path=test_vcf,
                annotation_databases=[clinvar_parquet],
                output_path=output_path
            )
            
            if result and output_path.exists():
                print(f"✅ Annotation completed: {output_path}")
                
                # Analyze pathogenicity in annotated results
                annotated_df = pl.read_parquet(output_path)
                annotated_stats = extract_pathogenicity_stats(annotated_df)
                
                print(f"\n📊 Annotated Results Analysis:")
                print(f"Total annotated variants: {len(annotated_df):,}")
                
                annotated_pathogenic = annotated_stats['pathogenic'] + annotated_stats['likely_pathogenic']
                annotated_benign = annotated_stats['benign'] + annotated_stats['likely_benign']
                annotated_vus = annotated_stats['uncertain_significance']
                
                print(f"🔴 Pathogenic variants found: {annotated_pathogenic}")
                print(f"🟢 Benign variants found: {annotated_benign}")
                print(f"🟡 VUS variants found: {annotated_vus}")
                
                if annotated_pathogenic > 0 or annotated_benign > 0 or annotated_vus > 0:
                    print("🎉 SUCCESS: Pathogenicity information is now properly captured!")
                else:
                    print("⚠️  No pathogenicity matches found in this sample")
            else:
                print("❌ Annotation failed")
        else:
            print(f"⚠️  Test VCF not found: {test_vcf}")
        
        print("\n" + "="*50)
        print("✨ Demo completed! The annotation system now provides:")
        print("   • Comprehensive pathogenicity analysis")
        print("   • Clinical significance statistics")
        print("   • User-friendly reporting with emojis")
        print("   • Detailed breakdown by pathogenicity category")
        print("   • Warning when coverage is low")
        print("\nNo more disgraceful missing pathogenicity info! 🎯")
        
        action.add_success_fields(
            source_pathogenicity_stats=stats,
            demo_completed=True
        )


if __name__ == "__main__":
    demo_pathogenicity_analysis()