"""
Preparation pipelines for genomic data sources.

This module provides pipelines for downloading, converting, splitting, and uploading
genomic data from various sources (ClinVar, Ensembl, dbSNP, gnomAD).
"""

import os
from pathlib import Path
from typing import Optional

from pipefunc import Pipeline
from eliot import start_action

from genobear.pipelines.preparation.vcf_downloader import (
    list_paths,
    download_path,
    convert_to_parquet,
    validate_downloads_and_parquet,
)
from genobear.pipelines.preparation.vcf_parquet_splitter import make_parquet_splitting_pipeline
from genobear.pipelines.preparation.huggingface_uploader import (
    make_hf_upload_pipeline,
    upload_to_hf_if_changed,
    collect_parquet_files,
)
from pycomfort.logging import to_nice_stdout, to_nice_file
from genobear.config import get_default_workers
from genobear.pipelines.runtime import run_pipeline


class PreparationPipelines:
    """Pipelines for preparing genomic data from various sources.
    
    This class provides static methods for:
    - Building pipelines for different data sources (ClinVar, Ensembl, dbSNP, gnomAD)
    - Downloading, converting, and splitting VCF data
    - Uploading processed data to Hugging Face Hub
    - Executing pipelines with proper configuration
    """
    
    @staticmethod
    def _vcf_base() -> Pipeline:
        """Internal: base VCF download + convert + validate pipeline."""
        return Pipeline(
            [list_paths, download_path, convert_to_parquet, validate_downloads_and_parquet],
            print_error=True,
        )
    
    @staticmethod
    def clinvar(with_splitting: bool = False) -> Pipeline:
        """Get a pipeline for downloading and processing ClinVar data.
        
        Args:
            with_splitting: If True, includes variant splitting by TSA
            
        Returns:
            Configured pipeline with ClinVar defaults
        """
        if with_splitting:
            # Compose VCF download pipeline with splitting pipeline
            vcf_pipeline = PreparationPipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = PreparationPipelines._vcf_base()
        
        # Set ClinVar defaults
        clinvar_defaults = {
            "url": "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/",
            "pattern": r"clinvar\.vcf\.gz$",
            "name": "clinvar",
            "file_only": True,
            "check_files": True,
            "expiry_time": 7 * 24 * 3600,  # 7 days
        }
        
        if with_splitting:
            clinvar_defaults["explode_snv_alt"] = True
        
        pipeline.update_defaults(clinvar_defaults)
        return pipeline
    
    @staticmethod
    def ensembl(with_splitting: bool = False) -> Pipeline:
        """Get a pipeline for downloading and processing Ensembl VCF data.
        
        Args:
            with_splitting: If True, includes variant splitting by TSA
            
        Returns:
            Configured pipeline with Ensembl defaults
        """
        if with_splitting:
            # Compose VCF download pipeline with splitting pipeline
            vcf_pipeline = PreparationPipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = PreparationPipelines._vcf_base()
        
        # Set Ensembl VCF defaults
        ensembl_defaults = {
            "url": "https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/",
            "pattern": r"homo_sapiens-chr([^.]+)\.vcf\.gz$",
            "name": "ensembl_variations",
            "file_only": True,
            "check_files": True,
            "expiry_time": 7 * 24 * 3600,  # 7 days
        }
        
        if with_splitting:
            ensembl_defaults["explode_snv_alt"] = True
        
        pipeline.update_defaults(ensembl_defaults)
        return pipeline
    
    @staticmethod
    def dbsnp(with_splitting: bool = False, build: str = "GRCh38") -> Pipeline:
        """Get a pipeline for downloading and processing dbSNP data.
        
        Args:
            with_splitting: If True, includes variant splitting by TSA
            build: Genome build (GRCh38 or GRCh37)
            
        Returns:
            Configured pipeline with dbSNP defaults
        """
        if with_splitting:
            # Compose VCF download pipeline with splitting pipeline
            vcf_pipeline = PreparationPipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = PreparationPipelines._vcf_base()
        
        # Set dbSNP VCF defaults based on build
        if build == "GRCh38":
            base_url = "https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/"
            pattern = r"GCF_000001405\.40\.gz$"  # GRCh38.p14 (latest)
        elif build == "GRCh37":
            base_url = "https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/"
            pattern = r"GCF_000001405\.25\.gz$"  # GRCh37.p13
        else:
            raise ValueError(f"Unsupported build: {build}. Use 'GRCh38' or 'GRCh37'")
        
        dbsnp_defaults = {
            "url": base_url,
            "pattern": pattern,
            "name": f"dbsnp_{build.lower()}",
            "file_only": True,
            "check_files": True,
            "expiry_time": 30 * 24 * 3600,  # 30 days (dbSNP changes less frequently)
        }
        
        if with_splitting:
            dbsnp_defaults["explode_snv_alt"] = True
        
        pipeline.update_defaults(dbsnp_defaults)
        return pipeline
    
    @staticmethod
    def gnomad(with_splitting: bool = False, version: str = "v4") -> Pipeline:
        """Get a pipeline for downloading and processing gnomAD data.
        
        Args:
            with_splitting: If True, includes variant splitting by TSA
            version: gnomAD version (v3, v4)
            
        Returns:
            Configured pipeline with gnomAD defaults
        """
        if with_splitting:
            # Compose VCF download pipeline with splitting pipeline
            vcf_pipeline = PreparationPipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = PreparationPipelines._vcf_base()
        
        # Set gnomAD VCF defaults based on version
        if version == "v4":
            base_url = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/4.0/vcf/"
            pattern = r"gnomad\.v4\.0\..+\.vcf\.bgz$"
        elif version == "v3":
            base_url = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/3.1.2/vcf/"
            pattern = r"gnomad\.v3\.1\.2\..+\.vcf\.bgz$"
        else:
            raise ValueError(f"Unsupported version: {version}. Use 'v3' or 'v4'")
        
        gnomad_defaults = {
            "url": base_url,
            "pattern": pattern,
            "name": f"gnomad_{version}",
            "file_only": True,
            "check_files": True,
            "expiry_time": 30 * 24 * 3600,  # 30 days
        }
        
        if with_splitting:
            gnomad_defaults["explode_snv_alt"] = True
        
        pipeline.update_defaults(gnomad_defaults)
        return pipeline
    
    @staticmethod
    def parquet_splitter() -> Pipeline:
        """Get a standalone parquet splitting pipeline.
        
        This can be used to split already-downloaded parquet files by variant type.
        
        Returns:
            Pipeline for splitting parquet files by TSA (variant type)
        """
        return make_parquet_splitting_pipeline()
    
    @staticmethod
    def vcf_downloader() -> Pipeline:
        """Get a basic VCF download and processing pipeline.
        
        Returns:
            Pipeline for VCF download, validation, and conversion to parquet
        """
        return PreparationPipelines._vcf_base()
    
    @staticmethod
    def vcf_splitter() -> Pipeline:
        """Get a VCF download and splitting pipeline.
        
        Returns:
            Pipeline for VCF download, conversion, and variant splitting by TSA
        """
        # Compose VCF download pipeline with splitting pipeline
        vcf_pipeline = PreparationPipelines._vcf_base()
        splitting_pipeline = make_parquet_splitting_pipeline()
        return vcf_pipeline | splitting_pipeline
    
    @staticmethod
    def hf_uploader() -> Pipeline:
        """Get a Hugging Face Hub upload pipeline.
        
        Returns:
            Pipeline for uploading parquet files to HF Hub
        """
        return make_hf_upload_pipeline()
    
    @staticmethod
    def execute(
        pipeline: Pipeline,
        inputs: dict,
        output_names: Optional[set[str]] = None,
        run_folder: Optional[str | Path] = None,
        return_results: bool = True,
        show_progress: str | bool = "rich",
        parallel: Optional[bool] = None,
        download_workers: Optional[int] = None,
        workers: Optional[int] = None,
        parquet_workers: Optional[int] = None,
        executors: Optional[dict] = None,
    ):
        """Execute a pipefunc Pipeline with GENOBEAR executors configuration."""
        return run_pipeline(
            pipeline=pipeline,
            inputs=inputs,
            output_names=output_names,
            run_folder=run_folder,
            return_results=return_results,
            show_progress=show_progress,
            parallel=parallel,
            download_workers=download_workers,
            workers=workers,
            parquet_workers=parquet_workers,
            executors=executors,
        )
    
    @staticmethod
    def download_clinvar(
        dest_dir: Optional[Path] = None,
        with_splitting: bool = False,
        download_workers: Optional[int] = None,
        parquet_workers: Optional[int] = None,
        workers: Optional[int] = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str] = None,
        **kwargs
    ) -> dict:
        """Download ClinVar VCF files and convert to parquet.
        
        High-level convenience method that builds and executes the ClinVar pipeline.
        
        Args:
            dest_dir: Destination directory. If None, uses platformdirs cache
            with_splitting: If True, splits variants by type (TSA)
            download_workers: Number of download workers
            parquet_workers: Number of parquet processing workers
            workers: Number of general workers
            log: Enable logging
            timeout: Download timeout in seconds
            run_folder: Optional run folder for caching
            **kwargs: Additional pipeline inputs
            
        Returns:
            Pipeline results dictionary
        """
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_clinvar.json", log_dir / "download_clinvar.log")
        
        with start_action(
            action_type="download_clinvar",
            dest_dir=str(dest_dir) if dest_dir else None,
            with_splitting=with_splitting
        ):
            pipeline = PreparationPipelines.clinvar(with_splitting=with_splitting)
            
            inputs = {}
            if dest_dir is not None:
                inputs["dest_dir"] = dest_dir
            if timeout is not None:
                inputs["timeout"] = timeout
            inputs.update(kwargs)
            
            return PreparationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=None,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich" if log else False,
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )
    
    @staticmethod
    def download_ensembl(
        dest_dir: Optional[Path] = None,
        with_splitting: bool = False,
        download_workers: Optional[int] = None,
        parquet_workers: Optional[int] = None,
        workers: Optional[int] = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str] = None,
        pattern: Optional[str] = None,
        url: Optional[str] = None,
        **kwargs
    ) -> dict:
        """Download Ensembl VCF files and convert to parquet.
        
        High-level convenience method that builds and executes the Ensembl pipeline.
        
        Args:
            dest_dir: Destination directory. If None, uses platformdirs cache
            with_splitting: If True, splits variants by type (TSA)
            download_workers: Number of download workers
            parquet_workers: Number of parquet processing workers
            workers: Number of general workers
            log: Enable logging
            timeout: Download timeout in seconds
            run_folder: Optional run folder for caching
            pattern: Regex pattern to filter files
            url: Base URL for Ensembl data
            **kwargs: Additional pipeline inputs
            
        Returns:
            Pipeline results dictionary
        """
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_ensembl.json", log_dir / "download_ensembl.log")
        
        with start_action(
            action_type="download_ensembl",
            dest_dir=str(dest_dir) if dest_dir else None,
            with_splitting=with_splitting,
            pattern=pattern
        ):
            pipeline = PreparationPipelines.ensembl(with_splitting=with_splitting)
            
            inputs = {}
            if dest_dir is not None:
                inputs["dest_dir"] = dest_dir
            if timeout is not None:
                inputs["timeout"] = timeout
            if pattern is not None:
                inputs["pattern"] = pattern
            if url is not None:
                inputs["url"] = url
            inputs.update(kwargs)
            
            return PreparationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=None,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich" if log else False,
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )
    
    @staticmethod
    def download_dbsnp(
        dest_dir: Optional[Path] = None,
        build: str = "GRCh38",
        with_splitting: bool = False,
        download_workers: Optional[int] = None,
        parquet_workers: Optional[int] = None,
        workers: Optional[int] = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str] = None,
        **kwargs
    ) -> dict:
        """Download dbSNP VCF files and convert to parquet.
        
        Args:
            dest_dir: Destination directory. If None, uses platformdirs cache
            build: Genome build (GRCh38 or GRCh37)
            with_splitting: If True, splits variants by type (TSA)
            download_workers: Number of download workers
            parquet_workers: Number of parquet processing workers
            workers: Number of general workers
            log: Enable logging
            timeout: Download timeout in seconds
            run_folder: Optional run folder for caching
            **kwargs: Additional pipeline inputs
            
        Returns:
            Pipeline results dictionary
        """
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_dbsnp.json", log_dir / "download_dbsnp.log")
        
        with start_action(
            action_type="download_dbsnp",
            dest_dir=str(dest_dir) if dest_dir else None,
            build=build,
            with_splitting=with_splitting
        ):
            pipeline = PreparationPipelines.dbsnp(with_splitting=with_splitting, build=build)
            
            inputs = {}
            if dest_dir is not None:
                inputs["dest_dir"] = dest_dir
            if timeout is not None:
                inputs["timeout"] = timeout
            inputs.update(kwargs)
            
            return PreparationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=None,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich" if log else False,
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )
    
    @staticmethod
    def download_gnomad(
        dest_dir: Optional[Path] = None,
        version: str = "v4",
        with_splitting: bool = False,
        download_workers: Optional[int] = None,
        parquet_workers: Optional[int] = None,
        workers: Optional[int] = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str] = None,
        **kwargs
    ) -> dict:
        """Download gnomAD VCF files and convert to parquet.
        
        Args:
            dest_dir: Destination directory. If None, uses platformdirs cache
            version: gnomAD version (v3 or v4)
            with_splitting: If True, splits variants by type (TSA)
            download_workers: Number of download workers
            parquet_workers: Number of parquet processing workers
            workers: Number of general workers
            log: Enable logging
            timeout: Download timeout in seconds
            run_folder: Optional run folder for caching
            **kwargs: Additional pipeline inputs
            
        Returns:
            Pipeline results dictionary
        """
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_gnomad.json", log_dir / "download_gnomad.log")
        
        with start_action(
            action_type="download_gnomad",
            dest_dir=str(dest_dir) if dest_dir else None,
            version=version,
            with_splitting=with_splitting
        ):
            pipeline = PreparationPipelines.gnomad(with_splitting=with_splitting, version=version)
            
            inputs = {}
            if dest_dir is not None:
                inputs["dest_dir"] = dest_dir
            if timeout is not None:
                inputs["timeout"] = timeout
            inputs.update(kwargs)
            
            return PreparationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=None,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich" if log else False,
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )
    
    @staticmethod
    def validate_ensembl(
        dest_dir: Optional[Path] = None,
        run_folder: Optional[str | Path] = None,
        vcf_local_paths: Optional[list[Path]] = None,
        parquet_paths: Optional[list[Path]] = None,
        **kwargs
    ) -> dict:
        """Validate already downloaded Ensembl VCF and parquet files.
        
        Args:
            dest_dir: Directory where files are stored, used to infer paths if vcf_local_paths or parquet_paths are not provided
            run_folder: Optional run folder for pipeline execution
            vcf_local_paths: List of paths to downloaded VCF files, if not provided will be inferred from dest_dir or default cache
            parquet_paths: List of paths to parquet files, if not provided will be inferred from dest_dir or default cache
            **kwargs: Additional arguments passed to pipeline.map()
            
        Returns:
            Validation results dictionary
        """
        from genobear.pipelines.preparation.vcf_downloader import list_paths, make_validation_pipeline
        from platformdirs import user_cache_dir
        
        # Fetch URLs for Ensembl data
        urls = list_paths(
            url="https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/",
            pattern=r"homo_sapiens-chr([^.]+)\.vcf\.gz$",
            file_only=True
        )
        
        # If paths are not provided, infer them from dest_dir or default cache location
        if vcf_local_paths is None or parquet_paths is None:
            if dest_dir is None:
                user_cache_path = Path(user_cache_dir(appname="genobear"))
                # Match the cache subfolder name used by the Ensembl pipeline's download step
                # (defaults set via PreparationPipelines.ensembl -> download_path's "name")
                default_name = PreparationPipelines.ensembl().functions[1].defaults.get("name", "downloads")
                dest_dir = user_cache_path / default_name
            else:
                dest_dir = Path(dest_dir)
            
            if vcf_local_paths is None:
                vcf_local_paths = []
                for url in urls:
                    filename = url.rsplit("/", 1)[-1]
                    local_path = dest_dir / filename
                    if local_path.exists():
                        vcf_local_paths.append(local_path)
            
            if parquet_paths is None:
                parquet_paths = []
                for url in urls:
                    filename = url.rsplit("/", 1)[-1].replace(".vcf.gz", ".parquet")
                    parquet_path = dest_dir / filename
                    if parquet_path.exists():
                        parquet_paths.append(parquet_path)
        
        # Run validation pipeline on provided or inferred files
        validation_pipeline = make_validation_pipeline()
        validation_inputs = {
            "urls": urls,
            "vcf_local": vcf_local_paths,
            "vcf_parquet_path": parquet_paths,
            **kwargs
        }
        with start_action(action_type="validate_ensembl") as val_action:
            val_action.log(message_type="info", step="start_validation", urls_count=len(urls), vcf_count=len(vcf_local_paths), parquet_count=len(parquet_paths))
            validation_results = validation_pipeline.map(
                inputs=validation_inputs,
                run_folder=run_folder,
                parallel=True,
                show_progress=True
            )
            val_action.log(message_type="info", step="validation_complete", results_keys=list(validation_results.keys()))
        
        return validation_results
    
    @staticmethod
    def split_existing_parquets(
        parquet_files: list[Path] | Path,
        explode_snv_alt: bool = True,
        write_to: Optional[Path] = None,
        workers: Optional[int] = None,
        log: bool = True,
        **kwargs
    ) -> dict:
        """Quick function to split existing parquet files by variant type.
        
        Args:
            parquet_files: Path or list of paths to parquet files
            explode_snv_alt: Whether to explode ALT column for SNV variants
            write_to: Output directory for split files
            workers: Number of parallel workers (default from env or cpu_count)
            log: Enable logging
            **kwargs: Additional arguments passed to pipeline.map()
            
        Returns:
            Pipeline results dictionary
        """
        if workers is None:
            workers = get_default_workers()
        
        pipeline = PreparationPipelines.parquet_splitter()
        
        if isinstance(parquet_files, Path):
            parquet_files = [parquet_files]
        
        inputs = {
            "vcf_parquet_path": parquet_files,
            "explode_snv_alt": explode_snv_alt,
            "write_to": write_to,
            **kwargs
        }
        
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "split_parquets.json", log_dir / "split_parquets.log")
        
        with start_action(action_type="split_existing_parquets", num_files=len(parquet_files), explode_snv_alt=explode_snv_alt):
            return PreparationPipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names={"split_variants_dict"},
                run_folder=None,
                return_results=True,
                show_progress="rich",
                parallel=True,
                workers=workers,
            )
    
    @staticmethod
    def upload_clinvar_to_hf(
        source_dir: Optional[Path] = None,
        repo_id: str = "just-dna-seq/clinvar",
        token: Optional[str] = None,
        pattern: str = "**/*.parquet",
        path_prefix: str = "data",
        workers: Optional[int] = None,
        log: bool = True,
        **kwargs
    ) -> dict:
        """Upload ClinVar parquet files to Hugging Face Hub.
        
        Only uploads files that differ in size from the remote versions,
        avoiding unnecessary data transfers.
        
        Args:
            source_dir: Source directory containing parquet files. If None, uses default cache
            repo_id: Hugging Face repository ID (default: just-dna-seq/clinvar)
            token: Hugging Face API token. If None, uses HF_TOKEN env variable
            pattern: Glob pattern for finding parquet files (default: **/*.parquet)
            path_prefix: Prefix for paths in the repository (default: data)
            workers: Number of parallel workers (default from env or cpu_count)
            log: Enable logging
            **kwargs: Additional arguments passed to upload function
            
        Returns:
            Pipeline results dictionary with upload status for each file
        """
        from platformdirs import user_cache_dir
        
        if workers is None:
            workers = get_default_workers()
        
        # Determine source directory
        if source_dir is None:
            user_cache_path = Path(user_cache_dir(appname="genobear"))
            default_name = "clinvar"
            source_dir = user_cache_path / default_name
        else:
            source_dir = Path(source_dir)
        
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "upload_clinvar.json", log_dir / "upload_clinvar.log")
        
        with start_action(
            action_type="upload_clinvar_to_hf",
            source_dir=str(source_dir),
            repo_id=repo_id,
            pattern=pattern
        ) as action:
            # Collect parquet files
            parquet_files = collect_parquet_files(source_dir, pattern=pattern)
            
            if not parquet_files:
                action.log(
                    message_type="warning",
                    reason="no_files_found",
                    source_dir=str(source_dir)
                )
                return {"uploaded_files": []}
            
            action.log(
                message_type="info",
                step="files_collected",
                num_files=len(parquet_files)
            )
            
            pipeline = PreparationPipelines.hf_uploader()
            
            # Prepare inputs for parallel upload
            # Note: path_in_repo is not in the mapspec, so we let the function compute it
            # from the filename. It will use "data/{filename}" by default.
            
            inputs = {
                "parquet_files": parquet_files,
                "repo_id": repo_id,
                "repo_type": "dataset",
                "path_in_repo": None,  # Let function compute from filename
                "token": token,
                **kwargs
            }
            
            # For upload pipeline, use simple default executor (not VCF-specific)
            from concurrent.futures import ThreadPoolExecutor
            upload_executor = ThreadPoolExecutor(max_workers=workers)
            
            try:
                results = PreparationPipelines.execute(
                    pipeline=pipeline,
                    inputs=inputs,
                    output_names={"uploaded_files"},
                    run_folder=None,
                    return_results=True,
                    show_progress="rich" if log else False,
                    parallel=True,
                    executors={"": upload_executor},  # Use simple default executor for upload
                )
            finally:
                upload_executor.shutdown(wait=True)
            
            # Summarize results
            uploaded_files = results.get("uploaded_files", [])
            if hasattr(uploaded_files, "output"):
                uploaded_files = uploaded_files.output
            if hasattr(uploaded_files, "ravel"):
                uploaded_files = uploaded_files.ravel().tolist()
            
            num_uploaded = sum(1 for r in uploaded_files if r.get("uploaded", False))
            num_skipped = len(uploaded_files) - num_uploaded
            
            action.log(
                message_type="summary",
                total_files=len(uploaded_files),
                uploaded=num_uploaded,
                skipped=num_skipped
            )
            
            return results

    @staticmethod
    def upload_ensembl_to_hf(
        source_dir: Optional[Path] = None,
        repo_id: str = "just-dna-seq/ensembl_variations",
        token: Optional[str] = None,
        pattern: str = "**/*.parquet",
        path_prefix: str = "data",
        workers: Optional[int] = None,
        log: bool = True,
        **kwargs
    ) -> dict:
        """Upload Ensembl variation parquet files to Hugging Face Hub.
        
        Only uploads files that differ in size from the remote versions,
        avoiding unnecessary data transfers.
        
        Args:
            source_dir: Source directory containing parquet files. If None, uses default cache
            repo_id: Hugging Face repository ID (default: just-dna-seq/ensembl_variations)
            token: Hugging Face API token. If None, uses HF_TOKEN env variable
            pattern: Glob pattern for finding parquet files (default: **/*.parquet)
            path_prefix: Prefix for paths in the repository (default: data)
            workers: Number of parallel workers (default from env or cpu_count)
            log: Enable logging
            **kwargs: Additional arguments passed to upload function
            
        Returns:
            Pipeline results dictionary with upload status for each file
        """
        from platformdirs import user_cache_dir
        
        if workers is None:
            workers = get_default_workers()
        
        # Determine source directory
        if source_dir is None:
            user_cache_path = Path(user_cache_dir(appname="genobear"))
            default_name = "ensembl_variations"
            source_dir = user_cache_path / default_name
        else:
            source_dir = Path(source_dir)
        
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "upload_ensembl.json", log_dir / "upload_ensembl.log")
        
        with start_action(
            action_type="upload_ensembl_to_hf",
            source_dir=str(source_dir),
            repo_id=repo_id,
            pattern=pattern
        ) as action:
            # Collect parquet files
            parquet_files = collect_parquet_files(source_dir, pattern=pattern)
            
            if not parquet_files:
                action.log(
                    message_type="warning",
                    reason="no_files_found",
                    source_dir=str(source_dir)
                )
                return {"uploaded_files": []}
            
            action.log(
                message_type="info",
                step="files_collected",
                num_files=len(parquet_files)
            )
            
            pipeline = PreparationPipelines.hf_uploader()
            
            # Prepare inputs for parallel upload
            # Note: path_in_repo is not in the mapspec, so we let the function compute it
            # from the filename. It will use "data/{filename}" by default.
            
            inputs = {
                "parquet_files": parquet_files,
                "repo_id": repo_id,
                "repo_type": "dataset",
                "path_in_repo": None,  # Let function compute from filename
                "token": token,
                **kwargs
            }
            
            # For upload pipeline, use simple default executor (not VCF-specific)
            from concurrent.futures import ThreadPoolExecutor
            upload_executor = ThreadPoolExecutor(max_workers=workers)
            
            try:
                results = PreparationPipelines.execute(
                    pipeline=pipeline,
                    inputs=inputs,
                    output_names={"uploaded_files"},
                    run_folder=None,
                    return_results=True,
                    show_progress="rich" if log else False,
                    parallel=True,
                    executors={"": upload_executor},  # Use simple default executor for upload
                )
            finally:
                upload_executor.shutdown(wait=True)
            
            # Summarize results
            uploaded_files = results.get("uploaded_files", [])
            if hasattr(uploaded_files, "output"):
                uploaded_files = uploaded_files.output
            if hasattr(uploaded_files, "ravel"):
                uploaded_files = uploaded_files.ravel().tolist()
            
            num_uploaded = sum(1 for r in uploaded_files if r.get("uploaded", False))
            num_skipped = len(uploaded_files) - num_uploaded
            
            action.log(
                message_type="summary",
                total_files=len(uploaded_files),
                uploaded=num_uploaded,
                skipped=num_skipped
            )
            
            return results

