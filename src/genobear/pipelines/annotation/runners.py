"""
Annotation pipelines for genomic VCF files.

This module provides pipelines for annotating VCF files with ensembl_variations data.
"""

from pathlib import Path
from typing import Optional

from pipefunc import Pipeline
from eliot import start_action

from genobear.pipelines.annotation.hf_dataset_downloader import (
    download_ensembl_from_hf,
    ensure_ensembl_cache,
)
from genobear.pipelines.annotation.vcf_parser import (
    extract_chromosomes_from_vcf,
    load_vcf_as_lazy_frame,
)
from genobear.pipelines.annotation.annotator import (
    find_annotation_parquet_files,
    annotate_vcf_with_ensembl,
    save_annotated_vcf,
)
from pycomfort.logging import to_nice_stdout, to_nice_file
from genobear.config import get_default_workers
from genobear.pipelines.runtime import run_pipeline


class AnnotationPipelines:
    """Pipelines for annotating VCF files with genomic databases.
    
    This class provides static methods for:
    - Downloading ensembl_variations reference data from HuggingFace Hub
    - Parsing VCF files to identify chromosomes
    - Annotating VCF data with reference data using lazy joins
    - Executing annotation pipelines with proper configuration
    """
    
    @staticmethod
    def _annotation_base() -> Pipeline:
        """Internal: base annotation pipeline."""
        return Pipeline(
            [
                ensure_ensembl_cache,
                load_vcf_as_lazy_frame,
                extract_chromosomes_from_vcf,
                find_annotation_parquet_files,
                annotate_vcf_with_ensembl,
            ],
            print_error=True,
        )
    
    @staticmethod
    def ensembl_annotation(with_save: bool = True) -> Pipeline:
        """Get a pipeline for annotating VCF with ensembl_variations data.
        
        Args:
            with_save: If True, includes saving the annotated result to parquet
            
        Returns:
            Configured pipeline for VCF annotation
        """
        if with_save:
            # Compose annotation pipeline with save step
            annotation_pipeline = AnnotationPipelines._annotation_base()
            save_pipeline = Pipeline([save_annotated_vcf], print_error=True)
            pipeline = annotation_pipeline | save_pipeline
        else:
            pipeline = AnnotationPipelines._annotation_base()
        
        # Set defaults
        defaults = {
            "variant_type": "SNV",
            "repo_id": "just-dna-seq/ensembl_variations",
        }
        
        pipeline.update_defaults(defaults)
        return pipeline
    
    @staticmethod
    def ensembl_downloader() -> Pipeline:
        """Get a standalone pipeline for downloading ensembl_variations data.
        
        Returns:
            Pipeline for downloading ensembl_variations from HuggingFace Hub
        """
        return Pipeline([download_ensembl_from_hf], print_error=True)
    
    @staticmethod
    def execute(
        pipeline: Pipeline,
        inputs: dict,
        output_names: Optional[set[str]] = None,
        run_folder: Optional[str | Path] = None,
        return_results: bool = True,
        show_progress: str | bool = "rich",
        parallel: Optional[bool] = None,
        workers: Optional[int] = None,
    ):
        """Execute a pipefunc Pipeline for annotation (sequential by default due to LazyFrames)."""
        # Annotation pipelines use LazyFrames which cannot be pickled for multiprocessing
        # So we run sequentially without executors
        use_parallel = parallel if parallel is not None else False
        
        if use_parallel:
            # If user explicitly requests parallel, use the annotation executor mode
            return run_pipeline(
                pipeline=pipeline,
                inputs=inputs,
                output_names=output_names,
                run_folder=run_folder,
                return_results=return_results,
                show_progress=show_progress,
                parallel=True,
                workers=workers,
                executor_mode="annotation",
            )
        else:
            # Run sequentially without executors (default for annotation)
            with start_action(
                action_type="execute_annotation_pipeline",
                run_folder=str(run_folder) if run_folder else None,
                parallel=False
            ):
                return pipeline.map(
                    inputs=inputs,
                    output_names=output_names,
                    parallel=False,
                    return_results=return_results,
                    show_progress=show_progress,
                    run_folder=run_folder,
                )
    
    @staticmethod
    def annotate_vcf(
        vcf_path: Path,
        output_path: Optional[Path] = None,
        cache_dir: Optional[Path] = None,
        repo_id: str = "just-dna-seq/ensembl_variations",
        variant_type: str = "SNV",
        token: Optional[str] = None,
        force_download: bool = False,
        workers: Optional[int] = None,
        log: bool = True,
        run_folder: Optional[str] = None,
        **kwargs
    ) -> dict:
        """Annotate a VCF file with ensembl_variations data.
        
        High-level convenience method that builds and executes the annotation pipeline.
        
        Args:
            vcf_path: Path to the input VCF file
            output_path: Path where to save annotated parquet file
            cache_dir: Local cache directory for ensembl_variations data
            repo_id: HuggingFace repository ID for ensembl_variations
            variant_type: Variant type to use for annotation (default: "SNV")
            token: HuggingFace API token
            force_download: Force re-download of ensembl_variations data
            workers: Number of parallel workers
            log: Enable logging
            run_folder: Optional run folder for caching
            **kwargs: Additional pipeline inputs
            
        Returns:
            Pipeline results dictionary
        """
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "annotate_vcf.json", log_dir / "annotate_vcf.log")
        
        with start_action(
            action_type="annotate_vcf",
            vcf_path=str(vcf_path),
            output_path=str(output_path) if output_path else None,
            variant_type=variant_type
        ):
            # Determine if we should save
            with_save = output_path is not None
            pipeline = AnnotationPipelines.ensembl_annotation(with_save=with_save)
            
            inputs = {
                "vcf_path": vcf_path,
                "cache_dir": cache_dir,
                "repo_id": repo_id,
                "variant_type": variant_type,
                "token": token,
                "force_download": force_download,
            }
            
            if with_save:
                inputs["output_path"] = output_path
            
            inputs.update(kwargs)
            
            # Determine which outputs to return
            if with_save:
                # When saving, explicitly request the annotated_vcf_path output
                output_names = {"annotated_vcf_path"}
            else:
                # When not saving, return the annotated lazy frame
                output_names = {"annotated_vcf_lazy"}
            
            results = AnnotationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=output_names,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich" if log else False,
                parallel=False,  # Annotation uses LazyFrames which cannot be pickled
                workers=workers,
            )
            
            # Extract actual values from Result objects if needed
            if results and isinstance(results, dict):
                # Handle pipefunc Result objects - extract .output if present
                extracted_results = {}
                for key, value in results.items():
                    if hasattr(value, "output"):
                        # It's a Result object, extract the output
                        extracted_results[key] = value.output
                    else:
                        extracted_results[key] = value
                results = extracted_results
            
            # Ensure annotated_vcf_path is in results when saving
            if with_save:
                if results and isinstance(results, dict) and "annotated_vcf_path" in results:
                    saved_path = results["annotated_vcf_path"]
                    # Convert to Path if it's a string
                    if isinstance(saved_path, str):
                        saved_path = Path(saved_path)
                    elif not isinstance(saved_path, Path):
                        # Try to convert other types
                        saved_path = Path(str(saved_path))
                    
                    results["annotated_vcf_path"] = saved_path
                    
                    # Verify the file was actually created
                    if not saved_path.exists() and output_path:
                        output_path_obj = Path(output_path)
                        if output_path_obj.exists():
                            # Fallback: use the provided output_path if it exists
                            results["annotated_vcf_path"] = output_path_obj
                elif output_path:
                    # If results don't contain the path but output_path was provided, use it
                    output_path_obj = Path(output_path)
                    if output_path_obj.exists():
                        if results is None:
                            results = {}
                        results["annotated_vcf_path"] = output_path_obj
            
            return results or {}
    
    @staticmethod
    def download_ensembl_reference(
        cache_dir: Optional[Path] = None,
        repo_id: str = "just-dna-seq/ensembl_variations",
        token: Optional[str] = None,
        force_download: bool = False,
        log: bool = True,
        **kwargs
    ) -> dict:
        """Download ensembl_variations reference data from HuggingFace Hub.
        
        Args:
            cache_dir: Local cache directory
            repo_id: HuggingFace repository ID
            token: HuggingFace API token
            force_download: Force re-download even if cache exists
            log: Enable logging
            **kwargs: Additional pipeline inputs
            
        Returns:
            Pipeline results dictionary with cache path
        """
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_ensembl_ref.json", log_dir / "download_ensembl_ref.log")
        
        with start_action(
            action_type="download_ensembl_reference",
            repo_id=repo_id,
            force_download=force_download
        ):
            pipeline = AnnotationPipelines.ensembl_downloader()
            
            inputs = {
                "cache_dir": cache_dir,
                "repo_id": repo_id,
                "token": token,
                "force_download": force_download,
            }
            inputs.update(kwargs)
            
            return AnnotationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names={"ensembl_cache_path"},
                run_folder=None,
                return_results=True,
                show_progress="rich" if log else False,
                parallel=False,
            )

