"""Integration tests for the annotation pipeline."""

from pathlib import Path

import pytest
from eliot import start_action

from genobear.pipelines.annotation import AnnotationPipelines


@pytest.mark.integration
def test_annotation_pipeline_with_local_vcf(tmp_path: Path) -> None:
    """
    Integration test for VCF annotation pipeline.
    
    This test:
    1. Uses the small test VCF file
    2. Downloads ensembl_variations reference data (if not cached)
    3. Annotates the VCF with reference data
    4. Saves the result to a parquet file
    
    Note: This test requires network access to download reference data
    and may take several minutes on first run. Subsequent runs will use
    cached reference data.
    """
    # Use the small test VCF file
    vcf_path = Path(__file__).parent.parent / "data" / "tests" / "antku_small.vcf"
    
    assert vcf_path.exists(), f"Test VCF file not found: {vcf_path}"
    
    output_path = tmp_path / "annotated_test.parquet"
    
    with start_action(
        action_type="test_annotation_pipeline",
        vcf_path=str(vcf_path),
        output_path=str(output_path)
    ) as action:
        # Run annotation pipeline
        results = AnnotationPipelines.annotate_vcf(
            vcf_path=vcf_path,
            output_path=output_path,
            log=False,  # Disable logging for test
        )
        
        action.log(
            message_type="info",
            step="annotation_completed",
            result_keys=list(results.keys())
        )
        
        # Verify results
        assert "annotated_vcf_path" in results, "Expected annotated_vcf_path in results"
        
        result_path = results["annotated_vcf_path"]
        assert result_path.exists(), f"Annotated file not found: {result_path}"
        assert result_path.stat().st_size > 0, "Annotated file is empty"
        
        action.log(
            message_type="info",
            step="verification_passed",
            result_path=str(result_path),
            file_size_bytes=result_path.stat().st_size
        )


@pytest.mark.integration
def test_download_ensembl_reference(tmp_path: Path) -> None:
    """
    Test downloading ensembl_variations reference data.
    
    This test verifies that the reference data can be downloaded
    and cached properly.
    """
    cache_dir = tmp_path / "ensembl_cache"
    
    with start_action(
        action_type="test_download_reference",
        cache_dir=str(cache_dir)
    ) as action:
        # Download reference data
        results = AnnotationPipelines.download_ensembl_reference(
            cache_dir=cache_dir,
            log=False,  # Disable logging for test
        )
        
        action.log(
            message_type="info",
            step="download_completed",
            result_keys=list(results.keys())
        )
        
        # Verify results
        assert "ensembl_cache_path" in results, "Expected ensembl_cache_path in results"
        
        cache_path = results["ensembl_cache_path"]
        assert cache_path.exists(), f"Cache directory not found: {cache_path}"
        
        # Check that some parquet files were downloaded
        parquet_files = list(cache_path.rglob("*.parquet"))
        assert len(parquet_files) > 0, "No parquet files found in cache"
        
        action.log(
            message_type="info",
            step="verification_passed",
            cache_path=str(cache_path),
            num_parquet_files=len(parquet_files)
        )


@pytest.mark.integration
def test_annotation_pipeline_skips_download_if_cached() -> None:
    """
    Test that the annotation pipeline skips download if cache exists.
    
    This test runs the annotation twice and verifies that the second
    run uses the cached data without re-downloading.
    """
    vcf_path = Path(__file__).parent.parent / "data" / "tests" / "antku_small.vcf"
    assert vcf_path.exists(), f"Test VCF file not found: {vcf_path}"
    
    with start_action(action_type="test_cached_annotation") as action:
        # First run - may download reference data
        results1 = AnnotationPipelines.annotate_vcf(
            vcf_path=vcf_path,
            output_path=None,  # Don't save, just process
            log=False,
        )
        
        cache_path1 = results1.get("ensembl_cache_path_checked")
        
        action.log(
            message_type="info",
            step="first_run_completed",
            cache_path=str(cache_path1) if cache_path1 else None
        )
        
        # Second run - should use cached data
        results2 = AnnotationPipelines.annotate_vcf(
            vcf_path=vcf_path,
            output_path=None,
            log=False,
        )
        
        cache_path2 = results2.get("ensembl_cache_path_checked")
        
        action.log(
            message_type="info",
            step="second_run_completed",
            cache_path=str(cache_path2) if cache_path2 else None
        )
        
        # Verify both runs used the same cache
        assert cache_path1 == cache_path2, "Cache paths should be identical"
        
        action.log(
            message_type="info",
            step="verification_passed",
            message="Both runs used the same cache"
        )

