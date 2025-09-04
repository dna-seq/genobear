import re
from pathlib import Path
from typing import List

import pytest
from eliot import start_action

from genobear.downloaders.vcf_pipeline import pipeline, list_paths


@pytest.mark.integration
def test_vcf_pipeline_downloads_to_temp(tmp_path: Path) -> None:
    """Integration test for the `vcf_pipeline` using a temporary destination.

    Mirrors the logic from the module's __main__ while preferring a small index file
    (".tbi") to keep the test lightweight. Falls back to the larger VCF if needed.
    """
    base_url = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/"
    # Prefer a tiny index file first; fall back to the full VCF if index is unavailable
    preferred_pattern = r"clinvar\.vcf\.gz\.tbi$"
    fallback_pattern = r"clinvar\.vcf\.gz$"

    with start_action(action_type="test_run_download_pipeline", url=base_url, dest=str(tmp_path)):
        # Probe remote listing to decide which pattern to use (no mocking)
        preferred_urls = list_paths(url=base_url, pattern=preferred_pattern, file_only=True)
        pattern = preferred_pattern if preferred_urls else fallback_pattern

        # Run pipeline mirroring the __main__ example
        results = pipeline.map(
            inputs=dict(
                url=base_url,
                pattern=pattern,
                file_only=True,
                name="pytest_vcf_pipeline",
                dest_dir=tmp_path,
                check_files=True,
                expiry_time=7 * 24 * 3600,
            ),
            output_names={"local", "lazy_frame", "parquet_path"},
            parallel=True,
            return_results=True,
        )

        local_result = results["local"]
        parquet_result = results["parquet_path"]
        lazy_frame_result = results["lazy_frame"]

        # Extract local paths
        local_out = local_result.output
        if hasattr(local_out, "ravel"):
            local_seq = local_out.ravel().tolist()
        elif isinstance(local_out, list):
            local_seq = local_out
        else:
            local_seq = [local_out]
        local_paths: List[Path] = [p if isinstance(p, Path) else Path(p) for p in local_seq]

        # Extract parquet paths
        parquet_out = parquet_result.output
        if hasattr(parquet_out, "ravel"):
            parquet_seq = parquet_out.ravel().tolist()
        elif isinstance(parquet_out, list):
            parquet_seq = parquet_out
        else:
            parquet_seq = [parquet_out]
        parquet_paths: List[Path] = [p if isinstance(p, Path) else Path(p) for p in parquet_seq]

        # Assertions with clear messages
        assert len(local_paths) >= 1, "Expected at least one downloaded file"
        assert len(parquet_paths) == len(local_paths), "Should have same number of parquet and local paths"
        
        vcf_files = []
        for i, (local_p, parquet_p) in enumerate(zip(local_paths, parquet_paths)):
            assert local_p.exists(), f"Downloaded file does not exist: {local_p}"
            assert local_p.stat().st_size > 0, f"Downloaded file is empty: {local_p}"
            assert local_p.parent.resolve() == tmp_path.resolve(), (
                f"File {local_p} not saved under the temporary destination {tmp_path}"
            )
            
            # Check if this is a VCF file that should have been converted
            is_vcf = ".vcf" in local_p.suffixes and not any(
                local_p.suffixes[-1] == ext for ext in [".tbi", ".csi", ".idx"]
            )
            
            if is_vcf:
                vcf_files.append((local_p, parquet_p))
                assert parquet_p.suffix == ".parquet", f"Expected .parquet extension for VCF conversion: {parquet_p}"
                assert parquet_p.exists(), f"Parquet file should exist for VCF: {parquet_p}"
                assert parquet_p.stat().st_size > 0, f"Parquet file should not be empty: {parquet_p}"
            else:
                # Non-VCF files should return the original path 
                assert parquet_p == local_p, f"Non-VCF file should return original path: {local_p} != {parquet_p}"
        
        # If we have VCF files, verify lazy frames work
        if vcf_files:
            lazy_frame_out = lazy_frame_result.output
            if hasattr(lazy_frame_out, "ravel"):
                lazy_frames = lazy_frame_out.ravel().tolist()
            elif isinstance(lazy_frame_out, list):
                lazy_frames = lazy_frame_out
            else:
                lazy_frames = [lazy_frame_out]
            
            # Check that we can work with the lazy frames (at least for VCF files)
            import polars as pl
            for i, (vcf_path, parquet_path) in enumerate(vcf_files):
                if i < len(lazy_frames):
                    lazy_frame = lazy_frames[i]
                    assert isinstance(lazy_frame, pl.LazyFrame), f"Expected LazyFrame for VCF: {vcf_path}"


