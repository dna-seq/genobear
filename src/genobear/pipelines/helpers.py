"""
Pipeline helper functions for common genomic data sources.

This module provides convenient functions to quickly construct pipelines
for popular genomic databases like ClinVar, Ensembl, dbSNP, etc.
"""

import os
from pathlib import Path
from typing import Optional, List

from pipefunc import Pipeline
from eliot import start_action

from genobear.pipelines.vcf_downloader import (
    list_paths,
    download_path,
    convert_to_parquet,
    validate_downloads_and_parquet,
)
from genobear.pipelines.vcf_parquet_splitter import make_parquet_splitting_pipeline
from pycomfort.logging import to_nice_stdout, to_nice_file
from genobear.config import get_default_workers, get_parquet_workers, get_download_workers
from genobear.pipelines.runtime import run_pipeline


class Pipelines:
    """Static class providing various genomic data processing pipelines as getters and execution methods."""
    
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
            vcf_pipeline = Pipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = Pipelines._vcf_base()
        
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
            vcf_pipeline = Pipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = Pipelines._vcf_base()
        
        # Set Ensembl VCF defaults
        ensembl_defaults = {
            "url": "https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/",
            "pattern": r"homo_sapiens-chr([^.]+)\.vcf\.gz$",
            "name": "ensembl_vcf",
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
            vcf_pipeline = Pipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = Pipelines._vcf_base()
        
        # Set dbSNP VCF defaults based on build
        if build == "GRCh38":
            base_url = "https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/"
            pattern = r"GCF_000001405\.38\.gz$"
        elif build == "GRCh37":
            base_url = "https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/"
            pattern = r"GCF_000001405\.25\.gz$"
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
            vcf_pipeline = Pipelines._vcf_base()
            splitting_pipeline = make_parquet_splitting_pipeline()
            pipeline = vcf_pipeline | splitting_pipeline
        else:
            pipeline = Pipelines._vcf_base()
        
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
        """Get a pipeline for splitting existing parquet files by variant type.
        
        Returns:
            Configured pipeline for parquet splitting
        """
        return make_parquet_splitting_pipeline()
    
    @staticmethod
    def vcf_downloader() -> Pipeline:
        """Get a basic VCF download and processing pipeline.
        
        Returns:
            Pipeline for VCF download, validation, and conversion to parquet
        """
        return Pipelines._vcf_base()
    
    @staticmethod
    def vcf_splitter() -> Pipeline:
        """Get a VCF download and splitting pipeline.
        
        Returns:
            Pipeline for VCF download, conversion, and variant splitting by TSA
        """
        # Compose VCF download pipeline with splitting pipeline
        vcf_pipeline = Pipelines._vcf_base()
        splitting_pipeline = make_parquet_splitting_pipeline()
        return vcf_pipeline | splitting_pipeline

    # Execution methods

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

    # Convenience execution methods for popular databases
    @staticmethod
    def download_clinvar(
        dest_dir: Optional[Path] = None,
        with_splitting: bool = False,
        download_workers: int = None,
        parquet_workers: int = None,
        workers: int = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str | Path] = None,
        **kwargs
    ) -> dict:
        """Quick function to download and process ClinVar data.
        
        Args:
            dest_dir: Destination directory for downloads
            with_splitting: Whether to split variants by TSA
            download_workers: Number of parallel download workers (default from GENOBEAR_DOWNLOAD_WORKERS or CPU count)
            parquet_workers: Number of parallel workers for parquet operations (default from GENOBEAR_PARQUET_WORKERS or 4)
            workers: Number of workers for general processing (default from GENOBEAR_WORKERS or CPU count)
            timeout: Timeout in seconds for downloads (default 1 hour)
            log: Enable logging
            run_folder: Optional run folder for pipeline execution
            **kwargs: Additional arguments passed to pipeline.map()
            
        Returns:
            Pipeline results dictionary
        """
        # Set defaults from environment variables
        if download_workers is None:
            download_workers = get_download_workers()
        if parquet_workers is None:
            parquet_workers = get_parquet_workers()
        if workers is None:
            workers = get_default_workers()
        
        timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
        
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_clinvar.json", log_dir / "download_clinvar.log")
        
        pipeline = Pipelines.clinvar(with_splitting=with_splitting)
        
        inputs = {
            "timeout": timeout,
            **kwargs
        }
        if dest_dir is not None:
            inputs["dest_dir"] = dest_dir
        
        if with_splitting:
            output_names = {"vcf_parquet_path", "split_variants_dict"}
        else:
            output_names = {"vcf_lazy_frame", "vcf_parquet_path"}
        
        with start_action(action_type="download_clinvar", with_splitting=with_splitting, dest_dir=str(dest_dir) if dest_dir else None):
            return Pipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=output_names,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich",
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )

    @staticmethod
    def download_ensembl(
        dest_dir: Optional[Path] = None,
        with_splitting: bool = True,
        download_workers: int = None,
        parquet_workers: int = None,
        workers: int = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str | Path] = None,
        **kwargs
    ) -> dict:
        """Quick function to download and process Ensembl VCF data.
        
        Args:
            dest_dir: Destination directory for downloads
            with_splitting: Whether to split variants by TSA
            download_workers: Number of parallel download workers (default from GENOBEAR_DOWNLOAD_WORKERS or CPU count)
            parquet_workers: Number of parallel workers for parquet operations (default from GENOBEAR_PARQUET_WORKERS or 4)
            workers: Number of workers for general processing (default from GENOBEAR_WORKERS or CPU count)
            log: Enable logging
            timeout: Timeout in seconds for downloads (default 1 hour)
            run_folder: Optional run folder for pipeline execution
            **kwargs: Additional arguments passed to pipeline.map()
            
        Returns:
            Pipeline results dictionary
        """
        # Set defaults from environment variables
        if download_workers is None:
            download_workers = get_download_workers()
        if parquet_workers is None:
            parquet_workers = get_parquet_workers()
        if workers is None:
            workers = get_default_workers()
        
        timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
        
        pipeline = Pipelines.ensembl(with_splitting=with_splitting)

        # Use pipeline defaults; only pass runtime parameters (list_paths runs inside pipeline)
        inputs = {
            "timeout": timeout,
            **kwargs
        }

        if dest_dir is not None:
            inputs["dest_dir"] = dest_dir

        # Execute via unified runtime to ensure env-configured executors are used
        with start_action(action_type="download_ensembl") as action:
            if log:
                action.log(message_type="info", step="start_pipeline", download_workers=download_workers, parquet_workers=parquet_workers, workers=workers)
            results = Pipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=None,
                run_folder=run_folder,
                return_results=True,
                show_progress=("rich" if log else False),
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )
            if log:
                action.log(message_type="info", step="pipeline_complete", results_keys=list(results.keys()))

        return results
    
    @staticmethod
    def validate_ensembl(
        dest_dir: Optional[Path] = None,
        run_folder: Optional[str | Path] = None,
        vcf_local_paths: Optional[List[Path]] = None,
        parquet_paths: Optional[List[Path]] = None,
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
        from genobear.pipelines.vcf_downloader import list_paths, make_validation_pipeline
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
                # (defaults set via Pipelines.ensembl -> download_path's "name")
                default_name = Pipelines.ensembl().functions[1].defaults.get("name", "downloads")
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
    def download_dbsnp(
        dest_dir: Optional[Path] = None,
        build: str = "GRCh38",
        with_splitting: bool = False,
        download_workers: int = None,
        parquet_workers: int = None,
        workers: int = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str | Path] = None,
        **kwargs
    ) -> dict:
        """Quick function to download and process dbSNP data.
        
        Args:
            dest_dir: Destination directory for downloads
            build: Genome build (GRCh38 or GRCh37)
            with_splitting: Whether to split variants by TSA
            download_workers: Number of parallel download workers (default from GENOBEAR_DOWNLOAD_WORKERS or CPU count)
            parquet_workers: Number of parallel workers for parquet operations (default from GENOBEAR_PARQUET_WORKERS or 4)
            workers: Number of workers for general processing (default from GENOBEAR_WORKERS or CPU count)
            timeout: Timeout in seconds for downloads (default 1 hour)
            log: Enable logging
            run_folder: Optional run folder for pipeline execution
            **kwargs: Additional arguments passed to pipeline.map()
            
        Returns:
            Pipeline results dictionary
        """
        # Set defaults from environment variables
        if download_workers is None:
            download_workers = get_download_workers()
        if parquet_workers is None:
            parquet_workers = get_parquet_workers()
        if workers is None:
            workers = get_default_workers()
        
        timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
        
        pipeline = Pipelines.dbsnp(with_splitting=with_splitting, build=build)
        
        inputs = {
            "timeout": timeout,
            **kwargs
        }
        if dest_dir is not None:
            inputs["dest_dir"] = dest_dir
        
        if with_splitting:
            output_names = {"vcf_parquet_path", "split_variants_dict"}
        else:
            output_names = {"vcf_lazy_frame", "vcf_parquet_path"}
        
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_dbsnp.json", log_dir / "download_dbsnp.log")
        
        with start_action(action_type="download_dbsnp", with_splitting=with_splitting, build=build, dest_dir=str(dest_dir) if dest_dir else None):
            return Pipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=output_names,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich",
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
        download_workers: int = None,
        parquet_workers: int = None,
        workers: int = None,
        log: bool = True,
        timeout: Optional[float] = None,
        run_folder: Optional[str | Path] = None,
        **kwargs
    ) -> dict:
        """Quick function to download and process gnomAD data.
        
        Args:
            dest_dir: Destination directory for downloads
            version: gnomAD version (v3, v4)
            with_splitting: Whether to split variants by TSA
            download_workers: Number of parallel download workers (default from GENOBEAR_DOWNLOAD_WORKERS or CPU count)
            parquet_workers: Number of parallel workers for parquet operations (default from GENOBEAR_PARQUET_WORKERS or 4)
            workers: Number of workers for general processing (default from GENOBEAR_WORKERS or CPU count)
            timeout: Timeout in seconds for downloads (default 1 hour)
            log: Enable logging
            run_folder: Optional run folder for pipeline execution
            **kwargs: Additional arguments passed to pipeline.map()
            
        Returns:
            Pipeline results dictionary
        """
        # Set defaults from environment variables
        if download_workers is None:
            download_workers = get_download_workers()
        if parquet_workers is None:
            parquet_workers = get_parquet_workers()
        if workers is None:
            workers = get_default_workers()
        
        timeout = float(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", 3600.0)) if timeout is None else timeout
        
        pipeline = Pipelines.gnomad(with_splitting=with_splitting, version=version)
        
        inputs = {
            "timeout": timeout,
            **kwargs
        }
        if dest_dir is not None:
            inputs["dest_dir"] = dest_dir
        
        if with_splitting:
            output_names = {"vcf_parquet_path", "split_variants_dict"}
        else:
            output_names = {"vcf_lazy_frame", "vcf_parquet_path"}
        
        if log:
            to_nice_stdout()
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            to_nice_file(log_dir / "download_gnomad.json", log_dir / "download_gnomad.log")
        
        with start_action(action_type="download_gnomad", with_splitting=with_splitting, version=version, dest_dir=str(dest_dir) if dest_dir else None):
            return Pipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names=output_names,
                run_folder=run_folder,
                return_results=True,
                show_progress="rich",
                parallel=True,
                download_workers=download_workers,
                parquet_workers=parquet_workers,
                workers=workers,
            )

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
        
        pipeline = Pipelines.parquet_splitter()
        
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
            return Pipelines.execute(
                pipeline=pipeline,
                inputs=inputs,
                output_names={"split_variants_dict"},
                run_folder=None,
                return_results=True,
                show_progress="rich",
                parallel=True,
                workers=workers,
            )



# All functionality has been moved to Pipelines class above.
# Use Pipelines.* methods instead of the old helper functions.